#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <math.h>
#include "lsmt.h"
#include "index.h"
#include "utils.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>



#define SIZE_THRESHOLD (2 * 1024 * 1024)

#define POOL_SIZE 4 
#define ZERO_LEVEL_MAX_SIZE (8 * 1024 * 1024) //maximum size for level 0 sstables.
#define SSTABLE_MERGE_LIMIT 4 //maximum number of sstable to merge

#define SST_FILENAME_MAX_LENGTH 256
#define SST_EXT ".sbrolf"
#define SST_INDEX_EXT ".prot"

/* Accrocchio temporaneo per dumpare la memtable su disco in multithreading */
static pthread_t thread_pool[POOL_SIZE];
static size_t thread_idx = 0;

static const size_t index_block_size = 4 * 1024; // 4KB

/* key + type + content_length. */
static const uint8_t RECORD_HEADER_SIZE = sizeof(sl_uint128_t) + sizeof(uint8_t) + sizeof(uint32_t);

static void *dump_to_disk(void *arg);

typedef struct dump_task {
  lsmt_t *lsmt;
  sl_t *memtable;
} dump_task_t;

typedef struct sst_file_paths {
  char *sst_file;
  char *index_file;
} sst_file_paths_t;

/*
 * Encode the data into an array of bytes prefixed by the type of the data and it's length.
 * | T | L | L | L | L | DATA |
 */
uint8_t *encode_data(uint8_t type, uint32_t length, void *data) {
  uint8_t *out = malloc(1 + 4 + length);
  out[0] = type;
  memcpy(out+1, &length, sizeof(uint32_t));
  memcpy(out+5, data, length); 

  return out;
}

static sst_file_paths_t new_sstable_filename(const char *dir) {
  static _Atomic(uint16_t) counter = 0;

  char filename[SST_FILENAME_MAX_LENGTH];
  sst_file_paths_t files;
  uint64_t epoch_ms = get_unix_epoch();
  uint16_t counter_val = atomic_fetch_add(&counter, 1); 

  snprintf(filename, sizeof(filename), "%s/db_%lu_%u", dir, epoch_ms, counter_val); 

  files.sst_file = malloc(strlen(filename) + strlen(SST_EXT) + 1);
  files.index_file = malloc(strlen(filename) + strlen(SST_EXT) + 1);

  snprintf(files.sst_file, strlen(filename) + strlen(SST_EXT) + 1, "%s%s", filename, SST_EXT);
  snprintf(files.index_file, strlen(filename) + strlen(SST_INDEX_EXT) + 1, "%s%s", filename, SST_INDEX_EXT);
  return files;
}

static bool read_record_header(FILE *fp, sl_uint128_t *key, uint8_t *type, uint32_t *length) {
 /* Key(16) + Type(1) + Len(4) = 21 Bytes */
  uint8_t buf[21]; 

  if (fread(buf, 1, 21, fp) != 21) {
    return false;
  }

  /* Unpack Buffer */
  memcpy(key, buf, 16);
  *type = buf[16];
  memcpy(length, buf + 17, 4);

  return true;
}


typedef struct written_record {
  sl_uint128_t key;
  uint32_t total_bytes;
  bool end;
} written_record_t;


/* Read one entry per SSTable, compare the keys and write the minimum
 * key into the new SSTable. This function increment only the position of the file who
 * contained the inserted record.
 * Returns the number of inserted bytes. */
static written_record_t write_min_record(FILE *out, FILE *sstables[], uint8_t sst_count) {
  written_record_t result;
  result.key.id = 0;
  result.end = true;

  uint8_t min_file_idx;
  uint8_t type;
  uint32_t length;
  for (uint8_t i = 0; i < sst_count; i++) {
    sl_uint128_t key;

    /* Current file has no more valid entries. */
    if (!read_record_header(sstables[i], &key, &type, &length)) {
      continue;
    }

    result.end = false;
    /* Temporary. Here i'm checking for the first occurrence. 
     * I should probably init the result.key at maximum value. */
    if (result.key.id == 0) {
      result.key = key;
      min_file_idx = i;
    }

    if (key_compare(key, result.key) < 0) {
      result.key = key;
      min_file_idx = i;
    }

    fseek(sstables[i], -RECORD_HEADER_SIZE, SEEK_CUR);
  }

  if (result.end) return result;

  FILE *fp = sstables[min_file_idx];
  uint32_t entry_length = RECORD_HEADER_SIZE + length; 
  uint8_t *record = malloc(sizeof(uint8_t) * entry_length); 

  /* Invalid/empty file. TODO. */
  if (fread(record, sizeof(uint8_t), entry_length, fp) != entry_length) {
    result.end = true;
    return result;
  }

  /* Increment the file pointer because i'im inserting its entry. */
  //fseek(fp, header_size, SEEK_CUR);

  if (fwrite(record, sizeof(uint8_t), entry_length, out) != entry_length) {
    perror("[MERGE] Failed to write records to disk !");
    exit(1);
  }
  free(record);

  result.total_bytes = entry_length;
  return result;
}

static sst_metadata_record_t sst_merge(sst_metadata_record_t *records[], uint8_t length, const char *db_path) {
  sst_file_paths_t file_paths = new_sstable_filename(db_path);

  FILE *fp[length]; // Local file handles for compaction only
  
  for (uint8_t i = 0; i < length; i++) {
    fp[i] = fopen(records[i]->sstable_filename, "rb");
    if (!fp[i]) {
      perror("Failed to open existing sstable for merge!");
      exit(1); 
    }
    /* User large buffer for sequential merge scan */
    setvbuf(fp[i], NULL, _IOFBF, 128 * 1024);
  }

  FILE *new_file = fopen(file_paths.sst_file, "wb");
  if (!new_file) {
    perror("Failed to open db file!");
    exit(1);
  }
  // Buffer for writing
  setvbuf(new_file, NULL, _IOFBF, 128 * 1024);

  written_record_t record;
  sl_uint128_t min_key = {0};
  sl_uint128_t max_key = {0};

  index_t *index = index_init(1024);
  size_t current_block_size = 0;
  uint64_t offset = 0;

  while ((record = write_min_record(new_file, fp, length)).end == false) {
    /* Temporary solution for getting the min key. Overflow risk. */
    if (offset == 0) min_key = record.key;

    if (current_block_size == 0) {
      if (index_add(index, record.key, offset) != 0) {
        printf("Failed to add index entry on sst merge!\n");
        exit(1);
      }
    }

    offset += record.total_bytes;
    current_block_size += record.total_bytes;
    max_key = record.key;

    if (current_block_size >= index_block_size) {
      current_block_size = 0;
    }
  }

  /* If the system crashes after syncing this new sstable to disk and
   * before updating the metadata file (which stores the list of all sstables)
   * then this new file will be ignored. This is way the deletion of the sstables
   * used here is postponed after the metadata file update.
   * */
  if (fflush(new_file) != 0) {
    perror("Failed to flush merged buffer.");
    fclose(new_file);
    exit(1);
  }

  if (fsync(fileno(new_file)) != 0) {
    perror("Failed to dump to disk the merged sstable.");
    fclose(new_file);
    exit(1);
  }

  fclose(new_file);

  for (uint8_t i = 0; i < length; i++) {
    fclose(fp[i]);
  }

  //snprintf(sst_path, sizeof(sst_path), "%s/%s", db_path, file_paths.index_file); 
  index_flush(index, file_paths.index_file);
  index_free(index);

  sst_metadata_record_t metadata = create_sst_metadata(records[0]->id,
                                                       (records[0]->level)+1,
                                                       offset,
                                                       min_key, max_key,
                                                       file_paths.sst_file,
                                                       file_paths.index_file);

  free(file_paths.sst_file);
  free(file_paths.index_file); 
  return metadata;
}

/* Iterate over all the tiers and compact the sstables
 * whose sum of their size exceed the tier threshold. */
static int sst_compact(sst_metadata_t *sst_meta, const char *db_path, pthread_mutex_t *mutex) {

  /* This buffer holds the merged sstables from all the tier processed
   * in this cycle. They are holded until the metadata are flushed to disk
   * to avoid data loss.
   */
  sst_metadata_record_t *sst_to_delete[256];
  int delete_count = 0;
  bool must_flush = false;

  pthread_mutex_lock(mutex);
  uint8_t highest_tier = sst_meta->highest_tier;
  pthread_mutex_unlock(mutex);

  for (uint8_t tier = 0; tier < highest_tier; tier++) {
    pthread_mutex_lock(mutex);
    double tier_size = sst_meta->tier_size[tier];
    double tier_size_threshold = ZERO_LEVEL_MAX_SIZE * pow(10, tier);

    if (tier_size > tier_size_threshold) {
      uint8_t j = 0;
      double tot_size = 0;
      double size_delta = tier_size - tier_size_threshold;
      sst_metadata_record_t *records[SSTABLE_MERGE_LIMIT];

      /* This is not the optimal way, i should choose the sstables so that the
       * sum of their size is the minimum above "max_level_size". Or some other
       * heuristic anyway. */
      while (j < SSTABLE_MERGE_LIMIT) {
        /* Stop if enough files are found AND enough size cleared. */
        if (j >= 2 && tot_size > size_delta) break;

        /* Check if tier is empty before popping. */
        if (sst_count(sst_meta, tier) == 0) break;

        sst_metadata_record_t *record = sst_metadata_pop(sst_meta, tier);  
        if (!record) break; 

        records[j] = record; 
        tot_size += records[j]->total_bytes;
        j++;
      }

      /* 
       * Merging 1 or zero files makes no sense.
       */
      if (j < 2) {
        //TODO: push back the popped records into the metadata list.
        printf("Tried to merge %d files!, quitting.\n", j);
        pthread_mutex_unlock(mutex);
        exit(1);
        continue; 
      }
 
      /* Merge the selected sstables into a new one with incremented tier. 
       * If returns any error, then i must push back the records into the metadata list. (TODO) */
      pthread_mutex_unlock(mutex);

      sst_metadata_record_t new_sst = sst_merge(records, j, db_path);

      pthread_mutex_lock(mutex);
      sst_metadata_add(sst_meta, new_sst); 

      /* Store the sst to be deleted later on. */
      for (int k = 0; k < j; k++) {
        if (delete_count < 256) {
          sst_to_delete[delete_count++] = records[k];
        }
        else {
          /* if buffer is full then mark for file deletion
           * and release it immediately. */
          records[k]->old = true;
          sst_metadata_record_release(records[k]);
        }
      }

      must_flush = true;
      pthread_mutex_unlock(mutex);
    }
    else {
      pthread_mutex_unlock(mutex);
    }
  }

  if (must_flush == true) {
    pthread_mutex_lock(mutex);

    if (sst_metadata_flush(sst_meta) != 0) {
      fprintf(stderr, "Critical Error: Failed to flush metadata. Cannot delete old files.\n");
      exit(1);
    }
    pthread_mutex_unlock(mutex);

    /* Delete merged sst tables. */
    for (int k = 0; k < delete_count; k++) {
      sst_to_delete[k]->old = true;
      sst_metadata_record_release(sst_to_delete[k]);
    }
  }
  return 0;
}

static void *dump_to_disk(void *arg) {
  dump_task_t *task_args = (dump_task_t *)arg;
  lsmt_t *lsmt = task_args->lsmt;
  sl_t *memtable = task_args->memtable;
  free(task_args);

  if (!memtable) return NULL;

  node_t *node = memtable->bottom_level->next;
  if (node == NULL) {
    // memtable is empty 
    sl_release(memtable);
    return NULL;
  }

  sst_file_paths_t files = new_sstable_filename(lsmt->db_path);
  FILE *fp = fopen(files.sst_file, "wb");
  if (!fp) {
    perror("Failed to open db file!");
    exit(1);
  }

  uint64_t offset = 0;
  sl_uint128_t min_key = node->key; 
  sl_uint128_t max_key = min_key;

  uint8_t *buf = NULL;
  size_t capacity = 256;

  buf = malloc(capacity);
  if (!buf) {
    perror("malloc on dumping sst to disk");
    exit(1);
  }

  index_t *index = index_init(1024);
  bool first_record = true;
  size_t current_block_size = 0;
  while(node != NULL) {
    /*Add index_block. */
    if (first_record || current_block_size == 0) {
      if (index_add(index, node->key, offset) != 0) {
        fprintf(stderr, "Failed to add index entry\n");
        exit(1);
      }
      first_record = false;
    }

    uint8_t *data = (uint8_t*)node->content;

    uint32_t record_content_size;
    memcpy(&record_content_size, data + sizeof(uint8_t), sizeof(uint32_t));

    size_t buf_size = RECORD_HEADER_SIZE + record_content_size;

    /* grow buffer if needed */
    if (buf_size > capacity) {
      size_t new_cap = buf_size * 2;
      uint8_t *tmp = realloc(buf, new_cap);
      if (!tmp) {
        perror("realloc buffer for kv record dump.");
        free(buf);
        fclose(fp);
        exit(1);
      }
      buf = tmp;
      capacity = new_cap;
    }


    memcpy(buf, &node->key, sizeof(sl_uint128_t));
    memcpy(buf + sizeof(sl_uint128_t), data, buf_size - sizeof(sl_uint128_t));

    if (fwrite(buf, buf_size, 1, fp) != 1) {
      perror("Failed to write kv record to disk");
      free(buf);
      exit(1);
    }

    offset += buf_size; 
    current_block_size += buf_size;

    if (current_block_size >= index_block_size) {
      current_block_size = 0;
    }

    max_key = node->key;
    node = node->next;
  }

  if (fflush(fp) != 0) {
    perror("Failed to flush memtable.");
    fclose(fp);
    exit(1);
  }

  if (fsync(fileno(fp)) != 0) {
    perror("Failed to dump memtable to disk.");
    fclose(fp);
    exit(1);
  }

  fclose(fp);
  free(buf);

  index_flush(index, files.index_file); 
  index_free(index);
  sl_release(memtable);

  sst_metadata_record_t metadata = create_sst_metadata(0, 0, offset, min_key, max_key, files.sst_file, files.index_file);
  free(files.sst_file);
  free(files.index_file);
 
  pthread_mutex_lock(&lsmt->metadata_lock);
  metadata.id = lsmt->sstable_id++;

  sst_metadata_add(lsmt->metadata, metadata);
  sst_metadata_flush(lsmt->metadata);

  pthread_mutex_unlock(&lsmt->metadata_lock);

  sst_compact(lsmt->metadata, lsmt->db_path, &lsmt->metadata_lock);
/*
  size_t total_size = memtable->size + sizeof(sl_t);
  printf("levels: %ld\n", memtable->levels);
  printf("allocated memory: %ld bytes (~ %.3f KB, %.3f MB)\n", total_size, (double)total_size/(1 << 10), (double)total_size/(1 << 20)); 
  printf("generated SSTable at %s; %ld bytes (~ %.3f KB, %.3f MB)\n", sst_file, offset, (double)offset/(1 << 10), (double)offset/(1 << 20));
  printf("generated index at %s; %ld bytes (~ %.3f KB, %.3f MB)\n", idx_file, offset, (double)offset/(1 << 10), (double)offset/(1 << 20));
  */
  return NULL;
}


static int ensureDir(const char *dir)
{
    int rv;
    struct stat sb;
    rv = stat(dir, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                printf("error: create directory '%s': %s", dir,
                       strerror(errno));
                return 1;
            }
        } else {
            printf("error: stat directory '%s': %s", dir, strerror(errno));
            return 1;
        }
    } else {
        if ((sb.st_mode & S_IFMT) != S_IFDIR) {
            printf("error: path '%s' is not a directory", dir);
            return 1;
        }
    }
    return 0;
}

lsmt_t *lsmt_init(const char *db_path) {
  lsmt_t *db = malloc(sizeof(lsmt_t));
  db->last_index = NULL;
  db->memtable = sl_init();
  if (pthread_mutex_init(&db->memtable_lock, NULL) != 0) {
    perror("LSMT INIT memtable_lock.");
    exit(1);
  }

  db->db_path = strdup(db_path);

  int res = ensureDir(db->db_path);
  if (res != 0) {
    perror("LSMT INIT");
    exit(1);
  }

  db->metadata = sst_metadata_init(db_path);
  if (pthread_mutex_init(&db->metadata_lock, NULL) != 0) {
    perror("LSMT INIT");
    exit(1);
  }

  return db;
}

void lsmt_flush(lsmt_t *lsmt) {
  pthread_mutex_lock(&lsmt->memtable_lock);
  for(size_t i = 0; i < thread_idx; i++) {
    pthread_join(thread_pool[i], NULL);
  }
  thread_idx = 0;

  dump_task_t *task_args = malloc(sizeof(dump_task_t));
  task_args->lsmt = lsmt;
  task_args->memtable = lsmt->memtable;

  dump_to_disk(task_args);
  lsmt->memtable = sl_init();
  pthread_mutex_unlock(&lsmt->memtable_lock);
}

void lsmt_free(lsmt_t *lsmt) {
  if (lsmt == NULL) return;
  if (lsmt->last_index != NULL) index_free(lsmt->last_index);
  if (lsmt->metadata) sst_metadata_free(lsmt->metadata); 
  if (lsmt->db_path) free(lsmt->db_path);
  pthread_mutex_destroy(&lsmt->metadata_lock);
  pthread_mutex_destroy(&lsmt->memtable_lock);

  free(lsmt);
}

int lsmt_insert(lsmt_t *lsmt, sl_uint128_t key, uint8_t *content, uint32_t size) {
  if (lsmt == NULL || lsmt->memtable == NULL) return -1;

  if (lsmt->memtable->size > SIZE_THRESHOLD) {
    pthread_mutex_lock(&lsmt->memtable_lock);
    //dump content to disk in a new thread.
    dump_task_t *task_args = malloc(sizeof(dump_task_t));
    task_args->lsmt = lsmt;

    /* Transfering ownership. */
    task_args->memtable = lsmt->memtable;
    

    if (thread_idx < POOL_SIZE) {
      pthread_create(&thread_pool[thread_idx++], NULL, dump_to_disk, task_args);
    }
    else {
      for(size_t i = 0; i < thread_idx; i++) {
        pthread_join(thread_pool[i], NULL);
      }
      thread_idx = 0;
      pthread_create(&thread_pool[thread_idx++], NULL, dump_to_disk, task_args);
    }

    lsmt->memtable = sl_init();
    pthread_mutex_unlock(&lsmt->memtable_lock);
  }
  int rv = sl_insert(lsmt->memtable, key, content, size);
  return rv;
}

static sst_iterator_t sst_iterator_create(sst_metadata_record_t *sst) {
  sst_iterator_t it = {0};
  sst_metadata_record_retain(sst);

  it.active = false;
  it.meta = sst;
  it.current_offset = 0;
  /* Init internal buffer. */
  it.buffer_cap = 1024;
  it.buffer = malloc(it.buffer_cap); 
  if (!it.buffer) { 
    perror("OOM");
    exit(1);
  } 

  /* active remains false until mounted! */
  return it;
}

static bool sst_meta_record_ensure_open(sst_metadata_record_t *meta) {
  if (!meta) return false;

  if (meta->fd == -1) {
    meta->fd = open(meta->sstable_filename, O_RDONLY);

    if (meta->fd == -1) {
      perror("Failed to open SST file");
      return false;
    }

    // Lazy Load Index if missing
    if (meta->cached_index == NULL) {
      meta->cached_index = index_load_from_disk(meta->sst_index_filename); 
      if (!meta->cached_index) {
        close(meta->fd);
        meta->fd = -1;
        return false;
      }
    }
  }

  return (meta->fd != -1 && meta->cached_index != NULL);
}

static void sst_iterator_close(sst_iterator_t *it) {
  if (it && it->meta) {
    sst_metadata_record_release(it->meta);
    it->meta = NULL;
    if (it->buffer) {
      free(it->buffer);
      it->buffer = NULL;
    }
  }
  if (it) {
    it->active = false;
  }
}

/* Returns true if header read successfully, false on EOF/Error */
static bool sst_read_header_at(int fd, uint64_t offset, 
    sl_uint128_t *out_key, size_t *out_total_size) {
  uint8_t buf[21]; // 16 (Key) + 1 (Type) + 4 (Len)

  if (pread(fd, buf, 21, offset) != 21) {
    return false;
  }

  memcpy(out_key, buf, 16);

  /* Skip Type value at byte 16 */
  uint32_t data_len;
  memcpy(&data_len, buf + 17, 4);

  if (out_total_size) {
    *out_total_size = 21 + data_len;
  }

  return true;
}

/* Returns true if the iterator is positioned and has valid data in range.
 * Returns false if the file does not overlap the range (optimization). */
static bool sst_iterator_seek(sst_iterator_t *it, sl_uint128_t start, sl_uint128_t end) {
  if (!it || !it->meta) return false;

  sst_metadata_record_t *sst = it->meta;

  /* 
   * If the SSTable is completely out of the query range, do not touch the disk.
   */
  if (key_compare(sst->max_key, start) < 0 || 
      key_compare(sst->min_key, end) > 0) {
    it->active = false;
    return false; 
  }

  if (!sst_meta_record_ensure_open(sst)) {
    it->active = false;
    return false;
  }

  /* Find closest index block. */
  uint64_t offset = index_lookup(sst->cached_index, start);

  sl_uint128_t key;
  size_t total_size;

  /* Move from the closest index block position to the closest
   * key of the provided range. */
  while (sst_read_header_at(sst->fd, offset, &key, &total_size)) {
    /* Went past the key range. */
    if (key_compare(key, end) > 0) {
      it->active = false;
      return false;
    }

    /* Found. */
    if (key_compare(key, start) >= 0) {
      it->current_offset = offset;
      it->end_key = end;
      it->active = true;
      it->buffered_record.valid = false;
      return true;
    }
    /* Move to the next header. */
    offset += total_size;
  }

  it->active = false;
  return false; 
}

static kv_raw_record_t sst_iterator_next(sst_iterator_t *it) {
  kv_raw_record_t record = {0};
  record.valid = false;

  if (!it || !it->active || !it->meta || it->meta->fd == -1) {
    return record; 
  }

  sl_uint128_t key;
  size_t total_size;

  if (!sst_read_header_at(it->meta->fd, it->current_offset, &key, &total_size)) {
    it->active = false;
    return record;
  }

  if (key_compare(key, it->end_key) > 0) {
    it->active = false;
    return record;
  }

  // Prepare Buffer (Reuse logic)
  if (it->buffer_cap < total_size) {
    size_t new_cap = (total_size > it->buffer_cap * 2) ? total_size : it->buffer_cap * 2;
    uint8_t *tmp = realloc(it->buffer, new_cap);
    if (!tmp) { 
      perror("OOM");
      exit(1);
    }
    it->buffer = tmp;
    it->buffer_cap = new_cap;
  }

  /* Read the full record into the buffer. */
  if (pread(it->meta->fd, it->buffer, total_size, it->current_offset) != total_size) {
    it->active = false;
    return record;
  }

  record.key = key;
  record.raw_data = it->buffer;
  record.total_size = total_size;
  record.valid = true;

  it->current_offset += total_size;
  return record;
}

static wrapper_sl_it_t wr_sl_it_create(sl_t *skiplist) {
  wrapper_sl_it_t wrapper = {0};
  wrapper.sl_it = sl_iterator_create(skiplist);
  wrapper.buffer_cap = 1024;
  wrapper.buffer = malloc(wrapper.buffer_cap); 
  if (!wrapper.buffer) { 
    perror("OOM");
    exit(1);
  }

  return wrapper;
}

static void wr_sl_ensure_buffered(wrapper_sl_it_t *wrap) {
  if (!wrap || !wrap->sl_it.active || wrap->buffered_record.valid) return;

  sl_kv_t raw = sl_iterator_next(&wrap->sl_it);

  if (!wrap->sl_it.active) {
    wrap->buffered_record.valid = false;
    return;
  }

  /* 
   * raw.content format: [Type (1 byte)] [Length (4 bytes)] [Value (N bytes)] 
   */
  uint8_t *content_ptr = (uint8_t*)raw.content;
  uint32_t val_len;
  // Read length from offset 1 (skip Type)
  memcpy(&val_len, content_ptr + 1, sizeof(uint32_t));

  /* 
   * Key (16) + Type (1) + Len (4) + Value (N)
   */
  size_t content_size = 1 + sizeof(uint32_t) + val_len;
  size_t total_size = sizeof(sl_uint128_t) + content_size;

  if (wrap->buffer_cap < total_size) {
    size_t new_cap = (total_size > wrap->buffer_cap * 2) ? 
      total_size : wrap->buffer_cap * 2;

    uint8_t *tmp = realloc(wrap->buffer, new_cap);
    if (!tmp) { 
      perror("OOM converting memtable record");
      exit(1);
    }
    wrap->buffer = tmp;
    wrap->buffer_cap = new_cap;
  }

  memcpy(wrap->buffer, &raw.key, sizeof(sl_uint128_t));
  memcpy(wrap->buffer + sizeof(sl_uint128_t), content_ptr, content_size);

  wrap->buffered_record.key = raw.key;
  wrap->buffered_record.raw_data = wrap->buffer;
  wrap->buffered_record.total_size = total_size;
  wrap->buffered_record.valid = true; 
}

void wr_sl_iterator_close(wrapper_sl_it_t *it) {
  sl_iterator_close(&it->sl_it);
  if (it->buffer) {
    free(it->buffer);
    it->buffer = NULL;
  }
  it->buffered_record.valid = false;
  return;
}

static bool wr_sl_iterator_seek(wrapper_sl_it_t *wrap, sl_uint128_t start, sl_uint128_t end) {
  if (!wrap) return false;

  bool active = sl_iterator_seek(&wrap->sl_it, start, end);

  /* Reset buffered_record since the position has changed. */
  wrap->buffered_record.valid = false;

  return active;
}

lsmt_iterator_t lsmt_iterator_create(lsmt_t *lsmt) {
  lsmt_iterator_t it = {0};
  it.lsmt = lsmt;
  it.active = true;
  it.sl_count = 0;

  pthread_mutex_lock(&lsmt->metadata_lock);

  /* Memtable Init. */
  pthread_mutex_lock(&lsmt->memtable_lock);
  bool use_memtable = (lsmt->memtable && lsmt->memtable->size > 0); 

  if (use_memtable) {
    it.sl_its = calloc(1, sizeof(wrapper_sl_it_t));
    it.sl_count = 1;
    it.sl_its[0] = wr_sl_it_create(lsmt->memtable);
  }
  pthread_mutex_unlock(&lsmt->memtable_lock);

  /* SSTable init. */
  it.sst_count = 0;
  sst_node_t *node = lsmt->metadata->list;

  /* TODO: replace with a function call to sst_metdata. */
  while(node) {
    it.sst_count++;
    node = node->next;
  }

  it.sst_its = calloc(it.sst_count, sizeof(sst_iterator_t));
  if (!it.sst_its) {
    perror("Failed to allocate sst iterator sources");
    pthread_mutex_unlock(&lsmt->metadata_lock);
    exit(1);
  }

  /* Create a snapshot of all the sstables on disk. */ 
  node = lsmt->metadata->list;
  size_t idx = 0;
  while(node) {
    it.sst_its[idx] = sst_iterator_create(node->content);
    idx++;
    node = node->next;
  }

  pthread_mutex_unlock(&lsmt->metadata_lock);
  return it;
}

void lsmt_iterator_close(lsmt_iterator_t *it) {
  if (it && it->active) {

    for (size_t i = 0; i < it->sl_count; i++) {
      wr_sl_iterator_close(&it->sl_its[i]);
    }

    for (size_t i = 0; i < it->sst_count; i++) {
      sst_iterator_close(&it->sst_its[i]);
    }

    if (it->sst_its) {
      free(it->sst_its);
      it->sst_its = NULL;
    }

    it->active = false;
  }
}

void lsmt_iterator_seek(lsmt_iterator_t *it, sl_uint128_t start, sl_uint128_t end) {
  if (!it) return;

  it->start_key = start;
  it->end_key = end;
  it->active = true;

  for (size_t i = 0; i < it->sl_count; i++) {
    wr_sl_iterator_seek(&it->sl_its[i], start, end);
  }

  for (size_t i = 0; i < it->sst_count; i++) {
    sst_iterator_seek(&it->sst_its[i], start, end);
  }
}

static void sst_ensure_buffered(sst_iterator_t *it) {
  if (!it->active || it->buffered_record.valid) return;

  it->buffered_record = sst_iterator_next(it);

  if (!it->buffered_record.valid) {
    it->active = false; 
  }
}

kv_raw_record_t lsmt_iterator_next(lsmt_iterator_t *it) {
  kv_raw_record_t result = {0};
  int sl_min_idx = -1;
  int sst_min_idx = -1;

  /* Lookup min key from memtables. */
  for (size_t i = 0; i < it->sl_count; i++) {
    wrapper_sl_it_t *wrapper = &it->sl_its[i];

    wr_sl_ensure_buffered(wrapper);
    if (!wrapper->buffered_record.valid) continue;

    if (sl_min_idx == -1) {
      sl_min_idx = (int)i;
    }
    else {
      sl_uint128_t k1 = wrapper->buffered_record.key;
      sl_uint128_t k2 = it->sl_its[sl_min_idx].buffered_record.key;
      if (key_compare(k1, k2) < 0) {
        sl_min_idx = i;
      }
    }
  }

  /* Lookup min key from SSTables. */
  for (size_t i = 0; i < it->sst_count; i++) {
    sst_iterator_t *sst_it = &it->sst_its[i]; 

    sst_ensure_buffered(sst_it);
    if (!sst_it->buffered_record.valid) continue;

    if (sst_min_idx == -1) {
      sst_min_idx = (int)i;
    }
    else {
      sl_uint128_t k1 = sst_it->buffered_record.key;
      sl_uint128_t k2 = it->sst_its[sst_min_idx].buffered_record.key;
      if (key_compare(k1, k2) < 0) {
        sst_min_idx = i;
      }
    }
  }

  kv_raw_record_t *winner = NULL;

  /* TODO: Compare minimum keys between SSTables and Memtables. */
  if (sl_min_idx == -1 && sst_min_idx == -1) {
    it->active = false;
    return result;
  }
  else if(sl_min_idx != -1 && sst_min_idx != -1) {
    sl_uint128_t k_sl = it->sl_its[sl_min_idx].buffered_record.key;
    sl_uint128_t k_sst = it->sst_its[sst_min_idx].buffered_record.key;

    if (key_compare(k_sl, k_sst) < 0) {
      winner = &it->sl_its[sl_min_idx].buffered_record;
    } else {
      winner = &it->sst_its[sst_min_idx].buffered_record;
    }
  }
  else if (sl_min_idx != -1) {
    winner = &it->sl_its[sl_min_idx].buffered_record;
  }
  else {
    winner = &it->sst_its[sst_min_idx].buffered_record;
  }

  result = *winner;
  /* Empty the buffer for next iterations. */
  winner->valid = false;
  return result;
}

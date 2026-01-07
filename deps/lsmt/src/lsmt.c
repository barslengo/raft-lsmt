#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
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
  return (fread(key, sizeof(sl_uint128_t), 1, fp) == 1 &&
        fread(type, sizeof(uint8_t), 1, fp) == 1 &&
        fread(length, sizeof(uint32_t), 1, fp) == 1); 
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

static sst_metadata_record_t sst_merge(sst_metadata_record_t records[], uint8_t length, const char *db_path) { 
  sst_file_paths_t file_paths = new_sstable_filename(db_path);

  FILE *fp[length];
  for (uint8_t i = 0; i < length; i++) {
    if (!(fp[i] = fopen(records[i].sstable_filename, "rb"))){
      perror("Failed to open existing sstable!");
      exit(1);
    }
  }

  FILE *new_file = fopen(file_paths.sst_file, "wb");
  if (!new_file) {
    perror("Failed to open db file!");
    exit(1);
  }

  uint64_t offset = 0;
  written_record_t record;
  sl_uint128_t min_key = {0};
  sl_uint128_t max_key = {0};
  index_t *index = NULL;

  while ((record = write_min_record(new_file, fp, length)).end == false) {
    /* Temporary solution for getting the min key. Overflow risk. */
    if (offset == 0) min_key = record.key;

    if (offset % index_block_size == 0) {
      index = index_add(index, record.key, offset);
    }

    offset += record.total_bytes;
    max_key = record.key;
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
  //snprintf(sst_path, sizeof(sst_path), "%s/%s", db_path, file_paths.index_file); 
  index_flush(index, file_paths.index_file);
  index_free(index);

  for (uint8_t i = 0; i < length; i++) {
    fclose(fp[i]);
    remove(records[i].sstable_filename);
    remove(records[i].sst_index_filename);
  }

  sst_metadata_record_t metadata = create_sst_metadata(records[0].id,
                                                       (records[0].level)+1,
                                                       offset,
                                                       min_key, max_key,
                                                       file_paths.sst_file,
                                                       file_paths.index_file);

  free(file_paths.sst_file);
  free(file_paths.index_file); 
  return metadata;
}


typedef struct {
  char sst_path[256];
  char idx_path[256];
} files_to_delete_t;

/* Iterate over all the tiers and compact the sstables
 * whose sum of their size exceed the tier threshold. */
static int sst_compact(sst_metadata_t *sst_meta, const char *db_path, pthread_mutex_t *mutex) {
  files_to_delete_t files_to_remove[128];
  int delete_count = 0;
  bool must_flush = false;

  uint8_t highest_tier = sst_meta->highest_tier;

  for (uint8_t tier = 0; tier < highest_tier; tier++) {
    pthread_mutex_lock(mutex);
    double tier_size = sst_meta->tier_size[tier];
    double tier_size_threshold = ZERO_LEVEL_MAX_SIZE * pow(10, tier);

    if (tier_size > tier_size_threshold) {
      uint8_t j = 0;
      double tot_size = 0;
      double size_delta = tier_size - tier_size_threshold;
      sst_metadata_record_t records[SSTABLE_MERGE_LIMIT];

      /* This is not the optimal way, i should choose the sstables so that the
       * sum of their size is the minimum above "max_level_size". Or some other
       * heuristic anyway. */
      while (j < 2 || (j < SSTABLE_MERGE_LIMIT && tot_size <= size_delta)) {
        sst_metadata_record_t record = sst_metadata_pop(sst_meta, tier);  

        if (record.total_bytes == 0) {
          break;
        }

        records[j] = record; 
        tot_size += records[j].total_bytes;
        j++;
      }

      /* Merge the selected sstables into a new one with incremented tier. 
       * If returns any error, then i must push back the records into the metadata list. (TODO) */
      pthread_mutex_unlock(mutex);

      sst_metadata_record_t new_sst = sst_merge(records, j, db_path);
      pthread_mutex_lock(mutex);

      sst_metadata_add(sst_meta, new_sst); 

      /* Store the files to be deleted later on. */
      for (int k = 0; k < j; k++) {
        strncpy(files_to_remove[delete_count].sst_path, records[k].sstable_filename, 255);
        strncpy(files_to_remove[delete_count].idx_path, records[k].sst_index_filename, 255);
        delete_count++;
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

    /* Delete the merged sstables from disk. */
    for (int k = 0; k < delete_count; k++) {
      remove(files_to_remove[k].sst_path);
      remove(files_to_remove[k].idx_path);
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

  index_t *index = NULL;
  uint64_t offset = 0;
  sl_uint128_t min_key = node->key; 
  sl_uint128_t max_key = min_key;

  while(node != NULL) {
    if (fwrite(&node->key, sizeof(sl_uint128_t), 1, fp) != 1) {
      perror("[MemDump - key] Failed to write key to disk !");
      exit(1);
    }

    uint8_t *data = (uint8_t*)node->content;
    uint32_t *size = (uint32_t *)(data+1);
    uint64_t buf_size = sizeof(uint8_t) + sizeof(uint32_t) + (*size);

    if (fwrite(data, buf_size, 1, fp) != 1) {
      perror("[MemDump - content] Failed to write to disk");
      exit(1);
    }

    if (offset % index_block_size == 0) {
      index = index_add(index, node->key, offset);
    }

    offset += RECORD_HEADER_SIZE + (*size);
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

  index_flush(index, files.index_file); 
  index_free(index);
  //sl_free(memtable);
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


static kv_record_t raw_data_to_record(sl_kv_t raw) {
  kv_record_t record = {0};

  uint8_t *buf = raw.content;  
  record.key = raw.key;
  memcpy(&record.data_type, buf, sizeof(uint8_t));
  buf += sizeof(uint8_t);

  memcpy(&record.data_len, buf, sizeof(uint32_t));
  buf += sizeof(uint32_t);

  record.data = malloc(record.data_len);
  if (!record.data) {
    printf("OOM cant allocate kv_record_t of size %u..\n", record.data_len);
    exit(1);
  }
  memcpy(record.data, buf, record.data_len);

  record.record_size = sizeof(sl_uint128_t) + sizeof(uint8_t) +
        sizeof(uint32_t) + record.data_len;

  return record;
}

static sst_iterator_t sst_iterator_create(sst_metadata_record_t *sst,
    sl_uint128_t start_key, sl_uint128_t end_key) {
  sst_metadata_record_t sst_info = *sst;

  sst_iterator_t it = {0};
  it.active = false;

  if (key_compare(sst_info.max_key, start_key) < 0 || 
      key_compare(sst_info.min_key, end_key) > 0) {
    return it;
  }

  /* Get the key offset reading the index file. */
  index_t *index = index_load_from_disk(sst_info.sst_index_filename); 
  uint64_t offset = index_lookup(index, start_key);
  index_free(index);

  /* Open the sstable file starting from the key offset. */
  FILE *fp = fopen(sst_info.sstable_filename, "rb");
  if (!fp) {
    return it;
  }

  fseek(fp, offset, SEEK_SET);

  it.active = true;
  it.fp = fp;
  it.start_key = start_key;
  it.end_key = end_key;
  return it;
}

static void sst_iterator_close(sst_iterator_t *it) {
  if (it && it->active && it->fp) {
    fclose(it->fp);
    it->fp = NULL;
    it->active = false;
  }
}

static kv_record_t sst_iterator_next(sst_iterator_t *it) {
  kv_record_t record = {0};
  if (!it || !it->active || !it->fp) {
      return record; 
  }

  FILE *fp = it->fp;

  sl_uint128_t fetched_key; 
  uint8_t type;
  uint32_t length;

  /* Fetch the key from the sstable. */
  while (read_record_header(fp, &fetched_key, &type, &length)) { 
    if (key_compare(fetched_key, it->end_key) > 0) {
      break;
    }

    if (key_compare(fetched_key, it->start_key) >= 0) {
      uint32_t record_size = sizeof(sl_uint128_t) + sizeof(uint8_t) +
        sizeof(uint32_t) + length;

      record.key = fetched_key;
      record.data_type = type;
      record.data_len = length;
      record.data = malloc(record.data_len);

      if(record.data) {
        if (fread(record.data, 1, record.data_len, fp) == record.data_len) {
          record.record_size = record_size;
          return record;
        } else {
          free(record.data); 
          record.data = NULL;
          break;
        }
      }
      else {
        printf("OOM");
        exit(1);
      }
    }
    else{
      fseek(fp, length, SEEK_CUR);
    }
  }
  sst_iterator_close(it);

  /* Reached EOF or key upper bound. */
  memset(&record, 0, sizeof(kv_record_t));
  return record;
}

/* The iterator blocks all inserts and merges until its closed. */
lsmt_iterator_t lsmt_iterator_create(lsmt_t *lsmt, sl_uint128_t start_key,
    sl_uint128_t end_key) {

  lsmt_iterator_t it = {0};
  it.start_key = start_key;
  it.end_key = end_key;

  it.lsmt = lsmt;
  it.sst_list = NULL;

  it.active = true;
  it.sst_it.active = false;
  it.memtable_it.active = false;

  pthread_mutex_lock(&lsmt->memtable_lock);
  if (lsmt->memtable) {
    it.memtable_it = sl_iterator_create(lsmt->memtable, start_key, end_key); 
  }
  else {
    pthread_mutex_lock(&lsmt->metadata_lock);
  }
  pthread_mutex_unlock(&lsmt->memtable_lock);
  return it;
}

void lsmt_iterator_close(lsmt_iterator_t *it) {
  if (it && it->active) {

    /* Close memtable iterator if exists. */
    sl_iterator_close(&it->memtable_it);

    /* Close sstable iterator. */
    sst_iterator_close(&it->sst_it);

    pthread_mutex_unlock(&it->lsmt->metadata_lock);
    it->active = false;
  }
}

kv_record_t lsmt_iterator_next(lsmt_iterator_t *it) {
  kv_record_t result = {0};
  if (!it->active) return result;

  /* Fetch from memtable. */
  if (it->memtable_it.active) {
    sl_kv_t sl_record = sl_iterator_next(&it->memtable_it); 
    if (it->memtable_it.active) return raw_data_to_record(sl_record);
    sl_iterator_close(&it->memtable_it);

    /* Start fetching from the sstables. */
    pthread_mutex_lock(&it->lsmt->metadata_lock);
    it->sst_list = it->lsmt->metadata->list;
  }

  /* Fetch from sstables. */
  while (it->sst_list != NULL) {
    if (!it->sst_it.active) {
      it->sst_it = sst_iterator_create(it->sst_list->content, it->start_key,
          it->end_key);
    }

    if (it->sst_it.active) {
      kv_record_t record = sst_iterator_next(&it->sst_it);
      if (it->sst_it.active) return record;
    }

    sst_iterator_close(&it->sst_it);
    it->sst_list = it->sst_list->next;
  }

  lsmt_iterator_close(it);
  return result;
}

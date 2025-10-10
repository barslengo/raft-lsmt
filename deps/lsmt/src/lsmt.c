#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
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


/* Iterate over all the tiers and compact the sstables
 * whose sum of their size exceed the tier threshold. */
static int sst_compact(sst_metadata_t *sst_meta, const char *db_path, pthread_mutex_t *mutex) {
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
      //printf("MERGING %d sstables, of total size = %f\n", j, tot_size);
      //fflush(stdout);
      pthread_mutex_unlock(mutex);

      sst_metadata_record_t new_sst = sst_merge(records, j, db_path);

      pthread_mutex_lock(mutex);
      sst_metadata_add(sst_meta, new_sst); 
      pthread_mutex_unlock(mutex);
    }
    else {
      pthread_mutex_unlock(mutex);
    }
  }
  return 0;
}

static void *dump_to_disk(void *arg) {
  dump_task_t *task_args = (dump_task_t *)arg;
  lsmt_t *lsmt = task_args->lsmt;
  sl_t *memtable = task_args->memtable;
  free(task_args);

  sst_file_paths_t files = new_sstable_filename(lsmt->db_path);

  FILE *fp = fopen(files.sst_file, "wb");
  if (!fp) {
    perror("Failed to open db file!");
    exit(1);
  }

  node_t *node = memtable->bottom_level->next;
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

  fclose(fp);
  index_flush(index, files.index_file); 
  index_free(index);
  sl_free(memtable);

  sst_metadata_record_t metadata = create_sst_metadata(lsmt->sstable_id++, 0, offset, min_key, max_key, files.sst_file, files.index_file);
  free(files.sst_file);
  free(files.index_file);
 
  pthread_mutex_lock(&lsmt->metadata_lock);

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
  for(size_t i = 0; i < thread_idx; i++) {
    pthread_join(thread_pool[i], NULL);
  }

  dump_task_t *task_args = malloc(sizeof(dump_task_t));
  task_args->lsmt = lsmt;
  task_args->memtable = lsmt->memtable;

  dump_to_disk(task_args);
  lsmt->memtable = sl_init();
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
  static size_t count = 0;
  if (lsmt == NULL || lsmt->memtable == NULL) return -1;

  /*
  if (count % 256 == 0) {
    printf("inserted %lu items.\n", count);
  }
  */
  if (lsmt->memtable->size > SIZE_THRESHOLD) {
    //dump content to disk in a new thread.
    dump_task_t *task_args = malloc(sizeof(dump_task_t));
    task_args->lsmt = lsmt;
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

    count = 0;
    lsmt->memtable = sl_init();
  }
  count++;

  //TODO: write ahead log
  return sl_insert(lsmt->memtable, key, content, size);
}

uint8_t *fetch_sst(sst_metadata_record_t *sst, sl_uint128_t key) {
  sst_metadata_record_t sst_info = *sst;
  uint8_t *content = NULL;

  /*
  printf("file found: keys [%lu%lu, %lu%lu], total_bytes=%ld, tier=%d, path=%s, index=%s\n",
         sst_info.min_key.id, sst_info.min_key.timestamp,
         sst_info.max_key.id, sst_info.max_key.timestamp,
         sst_info.total_bytes, sst_info.level,
         sst_info.sstable_filename, sst_info.sst_index_filename);
  fflush(stdout);
  */
  /* Get the key offset reading the index file. */
  index_t *index = index_load_from_disk(sst_info.sst_index_filename); 
  uint64_t offset = index_lookup(index, key);
  index_free(index);

  /* Open the sstable file starting from the key offset. */
  FILE *fp = fopen(sst_info.sstable_filename, "rb");
  fseek(fp, offset, SEEK_SET);

  sl_uint128_t fetched_key; 
  uint8_t type;
  uint32_t length;

  /* Fetch the key from the sstable. */
  while (read_record_header(fp, &fetched_key, &type, &length)) { 
    if (key_compare(key, fetched_key) == 0) {
      content = malloc(sizeof(uint8_t) * (1 + 4 + length));
      fseek(fp, -(1 + 4), SEEK_CUR);
      fread(content, sizeof(uint8_t), 1+4+length, fp);
      //printf("KEY=%ld\n", key.id);
      //fflush(stdout);
      break;
    }

    //printf("key=%ld, type=%d content_length=%d \n", fetched_key.id, type, length);
    //printf("type=%d content_length=%d \n", type, length);
    //fflush(stdout);

    fseek(fp, length, SEEK_CUR);
  }
  fclose(fp);
  return content;
}

uint8_t *lsmt_get(lsmt_t *lsmt, sl_uint128_t key) {
  if (lsmt == NULL) return NULL;
  uint8_t *content = NULL;

  if (lsmt->memtable) {
    //TODO: use bloom filter
    content = sl_get(lsmt->memtable, key);
  }

  if (content != NULL) return content;
  pthread_mutex_lock(&lsmt->metadata_lock); 

  sst_node_t *list = lsmt->metadata->list;
  while (list != NULL) {
    if (key_compare(key, list->content->min_key) >= 0 && 
      key_compare(key, list->content->max_key) <= 0) {
      content = fetch_sst(list->content, key);
      if (content) break;
    }
    list = list->next;
  }

  pthread_mutex_unlock(&lsmt->metadata_lock);
  return content;
}

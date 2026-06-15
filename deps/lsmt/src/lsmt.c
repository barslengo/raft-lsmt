#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#define _GNU_SOURCE
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

#define SIZE_THRESHOLD (16 * 1024 * 1024)

#define POOL_SIZE 2 
#define ZERO_LEVEL_MAX_SIZE (4 * 16 * 1024 * 1024) //maximum size for level 0 sstables.
#define SSTABLE_MERGE_LIMIT 4 //maximum number of sstable to merge

#define SST_FILENAME_MAX_LENGTH 256
#define SST_EXT ".sbrolf"
#define SST_INDEX_EXT ".prot"


/* Callback setter functions */
void lsmt_set_compaction_callback(lsmt_t *lsmt,
    lsmt_compaction_callback_t callback, void *user_data) {
  if (lsmt) {
    lsmt->compaction_callback = callback;
    lsmt->callback_user_data = user_data;
  }
}

void lsmt_set_memtable_flush_callback(lsmt_t *lsmt,
    lsmt_memtable_flush_callback_t callback, void *user_data) {
  if (lsmt) {
    lsmt->memtable_flush_callback = callback;
    lsmt->callback_user_data = user_data;
  }
}

static const size_t index_block_size = 4 * 1024; // 4KB

/* key + type + content_length. */
static const uint8_t RECORD_HEADER_SIZE = sizeof(sl_uint128_t) + sizeof(uint8_t) + sizeof(uint32_t);

static void *dump_to_disk(void *arg);
static void *compaction_daemon(void *arg);

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

static sst_metadata_record_t sst_merge(sst_metadata_record_t *records[], uint8_t length, const char *db_path) {
  sst_file_paths_t file_paths = new_sstable_filename(db_path);

  // 1. Setup the Set Iterator (Handles all reading, merging, and sorting)
  sst_k_iterators_t set_it = sst_k_iterators_create(records, length);

  // Seek from 0 to MAX to get everything
  sl_uint128_t min_range = {0};
  sl_uint128_t max_range = { .id = UINT64_MAX, .timestamp = UINT64_MAX };
  sst_k_iterators_seek(&set_it, min_range, max_range);

  // 2. Output File Setup
  FILE *new_file = fopen(file_paths.sst_file, "wb");
  if (!new_file) {
    perror("Failed to open new sst file");
    exit(1);
  }

  uint64_t input_bytes = 0;
  for (int k = 0; k < length; k++) {
    input_bytes += records[k]->total_bytes;
  }

  posix_fallocate(fileno(new_file), 0, input_bytes);
  setvbuf(new_file, NULL, _IOFBF, 1024 * 1024);

  index_t *index = index_init(1024);
  uint64_t offset = 0;
  size_t current_block_size = 0;

  sl_uint128_t min_key = {0};
  sl_uint128_t max_key = {0};
  bool first = true;

  kv_raw_record_t *rec;
  size_t bytes_written_since_sleep = 0; 

  while ((rec = sst_k_iterators_next(&set_it)) != NULL) {
    if (first) {
      min_key = rec->key;
      first = false;
    }
    max_key = rec->key;

    if (current_block_size == 0) {
      if (index_add(index, rec->key, offset) != 0) exit(1);
    }

    if (fwrite(rec->raw_data, 1, rec->total_size, new_file) != rec->total_size) {
      perror("Write failed during merge");
      exit(1);
    }

    offset += rec->total_size;
    current_block_size += rec->total_size;
    bytes_written_since_sleep += rec->total_size;

    if (current_block_size >= index_block_size) current_block_size = 0;

    /* Micro-sleep to throttle CPU and I/O, allowing main thread to regain control */
    if (bytes_written_since_sleep >= 1024 * 1024) { // Every 1 Megabyte written.
      usleep(1000);
      bytes_written_since_sleep = 0;
    }
  }

  if (fflush(new_file) != 0 || fsync(fileno(new_file)) != 0) {
    perror("Sync failed");
    fclose(new_file);
    exit(1);
  }

  fclose(new_file);
  sst_k_iterators_close(&set_it);

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
static int sst_compact(lsmt_t *lsmt, sst_metadata_t *sst_meta, const char *db_path, pthread_mutex_t *mutex) {
  uint64_t compaction_start_ts = get_monotonic_time_ms();
  uint32_t total_merged = 0;
  uint64_t total_input_bytes = 0;
  uint64_t total_output_bytes = 0;
  uint8_t compaction_level = 0;

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

  for (uint8_t tier = 0; tier <= highest_tier; tier++) {
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
      
      /* Track compaction metrics for this merge */
      total_merged += j;
      for (int k = 0; k < j; k++) {
        total_input_bytes += records[k]->total_bytes;
      }
      total_output_bytes += new_sst.total_bytes;
      compaction_level = new_sst.level;

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
    
    /* Invoke compaction callback if set */
    uint64_t end_ts = get_monotonic_time_ms();
    if (lsmt && lsmt->compaction_callback && total_merged > 0) {
      uint64_t duration_ms = end_ts - compaction_start_ts;

      lsmt->compaction_callback(lsmt->callback_user_data,
          get_unix_epoch(), duration_ms, total_merged, total_input_bytes,
          total_output_bytes, compaction_level);
    }
  }
  return 0;
}

static void *dump_to_disk(void *arg) {
  dump_task_t *task_args = (dump_task_t *)arg;
  lsmt_t *lsmt = task_args->lsmt;
  sl_t *memtable = task_args->memtable;
  free(task_args);

  uint64_t start_ts = get_monotonic_time_ms();

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

  /* Preallocate SIZE_THRESHOLD file size. */
  posix_fallocate(fileno(fp), 0, SIZE_THRESHOLD);

  /* 1MB buffer for fwrite */
  setvbuf(fp, NULL, _IOFBF, 1024 * 1024);

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

  sst_metadata_record_t metadata = create_sst_metadata(0, 0, offset, min_key, max_key, files.sst_file, files.index_file);
  free(files.sst_file);
  free(files.index_file);
 
  pthread_mutex_lock(&lsmt->metadata_lock);
  metadata.id = lsmt->sstable_id++;

  sst_metadata_add(lsmt->metadata, metadata);
  sst_metadata_flush(lsmt->metadata);

  pthread_mutex_unlock(&lsmt->metadata_lock);

  /* Unregister the immutable memtable. */
  pthread_rwlock_wrlock(&lsmt->memtable_rwlock);
  for (int i = 0; i < lsmt->immutable_count; i++) {
    if (lsmt->immutables[i] == memtable) {
      /* Shift remaining memtables */
      for (int j = i; j < lsmt->immutable_count - 1; j++) {
        lsmt->immutables[j] = lsmt->immutables[j+1];
      }
      lsmt->immutable_count--;
      break;
    }
  }
  pthread_rwlock_unlock(&lsmt->memtable_rwlock);
  sl_release(memtable);

  /* Signal the compaction daemon thread to run compaction */
  pthread_mutex_lock(&lsmt->compaction_mutex);
  lsmt->compaction_needed = true;
  pthread_cond_signal(&lsmt->compaction_cond);
  pthread_mutex_unlock(&lsmt->compaction_mutex);
  
  /* Invoke memtable flush callback */
  uint64_t end_ts = get_monotonic_time_ms();
  if (lsmt->memtable_flush_callback) {
    uint64_t duration_ms = end_ts - start_ts;

    lsmt->memtable_flush_callback(lsmt->callback_user_data,
        get_unix_epoch(), duration_ms, offset);
  }

  /* Notify the pool that this thread is finished */
  pthread_mutex_lock(&lsmt->thread_pool_mutex);
  lsmt->active_background_threads--;
  pthread_cond_signal(&lsmt->thread_pool_cond);
  pthread_mutex_unlock(&lsmt->thread_pool_mutex);
  
  return NULL;
}

static void *compaction_daemon(void *arg) {
  lsmt_t *lsmt = (lsmt_t *)arg;
  while (1) {
    pthread_mutex_lock(&lsmt->compaction_mutex);
    while (!lsmt->compaction_needed && !lsmt->stop_compaction) {
      pthread_cond_wait(&lsmt->compaction_cond, &lsmt->compaction_mutex);
    }

    if (lsmt->stop_compaction) {
      pthread_mutex_unlock(&lsmt->compaction_mutex);
      break;
    }

    lsmt->compaction_needed = false;
    pthread_mutex_unlock(&lsmt->compaction_mutex);

    sst_compact(lsmt, lsmt->metadata, lsmt->db_path, &lsmt->metadata_lock);
  }
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
  db->immutable_count = 0;

  for (int i = 0; i < MAX_IMMUTABLES; i++) {
    db->immutables[i] = NULL;
  }

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

  if (pthread_rwlock_init(&db->memtable_rwlock, &attr) != 0) {
    perror("LSMT INIT memtable_lock.");
    exit(1);
  }
  pthread_rwlockattr_destroy(&attr);

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

  pthread_mutex_init(&db->thread_pool_mutex, NULL);
  pthread_cond_init(&db->thread_pool_cond, NULL);
  db->active_background_threads = 0;
  
  /* Initialize callbacks to NULL */
  db->compaction_callback = NULL;
  db->memtable_flush_callback = NULL;
  db->callback_user_data = NULL;

  /* Initialize compaction daemon thread and synchronization primitives */
  db->stop_compaction = false;
  db->compaction_needed = false;
  if (pthread_mutex_init(&db->compaction_mutex, NULL) != 0) {
    perror("LSMT INIT compaction_mutex");
    exit(1);
  }
  if (pthread_cond_init(&db->compaction_cond, NULL) != 0) {
    perror("LSMT INIT compaction_cond");
    exit(1);
  }
  if (pthread_create(&db->compaction_thread, NULL, compaction_daemon, db) != 0) {
    perror("LSMT INIT compaction_thread");
    exit(1);
  }

  return db;
}

void lsmt_flush(lsmt_t *lsmt) {
  pthread_rwlock_wrlock(&lsmt->memtable_rwlock);
  sl_t *memtable_to_flush = lsmt->memtable;
  lsmt->memtable = sl_init();
  pthread_rwlock_unlock(&lsmt->memtable_rwlock);

  /* Wait for all background threads to finish. */
  pthread_mutex_lock(&lsmt->thread_pool_mutex);
  while (lsmt->active_background_threads > 0) {
    pthread_cond_wait(&lsmt->thread_pool_cond, &lsmt->thread_pool_mutex);
  }

  /* Execute final flush on main thread */
  lsmt->active_background_threads++;
  pthread_mutex_unlock(&lsmt->thread_pool_mutex);

  dump_task_t *task_args = malloc(sizeof(dump_task_t));
  task_args->lsmt = lsmt;
  task_args->memtable = memtable_to_flush;

  dump_to_disk(task_args);
}

void lsmt_free(lsmt_t *lsmt) {
  if (lsmt == NULL) return;

  /* Stop compaction daemon thread */
  pthread_mutex_lock(&lsmt->compaction_mutex);
  lsmt->stop_compaction = true;
  pthread_cond_signal(&lsmt->compaction_cond);
  pthread_mutex_unlock(&lsmt->compaction_mutex);

  pthread_join(lsmt->compaction_thread, NULL);
  pthread_mutex_destroy(&lsmt->compaction_mutex);
  pthread_cond_destroy(&lsmt->compaction_cond);

  if (lsmt->last_index != NULL) index_free(lsmt->last_index);
  if (lsmt->metadata) sst_metadata_free(lsmt->metadata); 
  if (lsmt->db_path) free(lsmt->db_path);
  pthread_mutex_destroy(&lsmt->metadata_lock);
  pthread_rwlock_destroy(&lsmt->memtable_rwlock);

  free(lsmt);
}

int lsmt_insert(lsmt_t *lsmt, sl_uint128_t key, uint8_t *content, uint32_t size) {
  if (lsmt == NULL || lsmt->memtable == NULL) return -1;

  sl_t *memtable_to_flush = NULL;

  pthread_rwlock_wrlock(&lsmt->memtable_rwlock);
  if (lsmt->memtable->size > SIZE_THRESHOLD) {
    memtable_to_flush = lsmt->memtable;
    lsmt->immutables[lsmt->immutable_count++] = memtable_to_flush;
    lsmt->memtable = sl_init();
  }
  int rv = sl_insert(lsmt->memtable, key, content, size);
  pthread_rwlock_unlock(&lsmt->memtable_rwlock);

  if (memtable_to_flush != NULL) {
    //dump content to disk in a new thread.
    dump_task_t *task_args = malloc(sizeof(dump_task_t));
    task_args->lsmt = lsmt;

    /* Transfering ownership. */
    task_args->memtable = memtable_to_flush;

    pthread_mutex_lock(&lsmt->thread_pool_mutex);

    /* Wait for a background thread slot. 
     * Because the memtable lock is RELEASED, other clients can 
     * keep writing to the active_memtable while we wait here! */
    while (lsmt->active_background_threads >= POOL_SIZE) {
      pthread_cond_wait(&lsmt->thread_pool_cond, &lsmt->thread_pool_mutex);
    }
    lsmt->active_background_threads++;
    pthread_mutex_unlock(&lsmt->thread_pool_mutex);

    /* Create a detached Threads */
    pthread_t t;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&t, &attr, dump_to_disk, task_args);
    pthread_attr_destroy(&attr);
  }

  return rv;
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
  if (!it) return;

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

  pthread_mutex_lock(&lsmt->metadata_lock);
  pthread_rwlock_rdlock(&lsmt->memtable_rwlock);

  /* Initialize Memtable Iterator. */
  if (lsmt->memtable && lsmt->memtable->size > 0) {
    it.sl_count = 1;
    it.sl_its = malloc(sizeof(wrapper_sl_it_t) * it.sl_count);
    it.sl_its[0] = wr_sl_it_create(lsmt->memtable);
  }

  /* Initialize SST Iterators. */
  size_t sst_count = 0;
  sst_node_t *node = lsmt->metadata->list;
  while(node) {
    sst_count++;
    node = node->next;
  }

  if (sst_count > 0) {
    sst_metadata_record_t **records = malloc(sizeof(sst_metadata_record_t*) * sst_count);
    node = lsmt->metadata->list;
    size_t i = 0;
    while(node) {
      records[i++] = node->content;
      node = node->next;
    }
    // Pass ownership of the array to the iterator (it copies pointers, we free the array)
    it.sst_list = sst_k_iterators_create(records, sst_count);
    free(records);
  } else {
    it.sst_list.count = 0;
  }

  pthread_rwlock_unlock(&lsmt->memtable_rwlock);
  pthread_mutex_unlock(&lsmt->metadata_lock);
  return it;
}

void lsmt_iterator_close(lsmt_iterator_t *it) {
  if (!it) return;


  if (it->sl_count > 0 && it->sl_its) {
    for (size_t i = 0; i < it->sl_count; i++) {
      wr_sl_iterator_close(&it->sl_its[i]);
    }
    free(it->sl_its);
    it->sl_its = NULL;
    it->sl_count = 0;
  }

  sst_k_iterators_close(&it->sst_list);
  it->active = false;
}

void lsmt_iterator_seek(lsmt_iterator_t *it, sl_uint128_t start, sl_uint128_t end) {
  if (!it) return;
  it->active = true;

  if (it->sl_count > 0) {
    for (size_t i = 0; i < it->sl_count; i++) {
      wr_sl_iterator_seek(&it->sl_its[i], start, end);
    }
  }

  if (it->sst_list.count > 0) {
    sst_k_iterators_seek(&it->sst_list, start, end);
  }
}

kv_raw_record_t lsmt_iterator_next(lsmt_iterator_t *it) {
  kv_raw_record_t result = {0};
  if (!it || !it->active) return result;

  /* Peek Memtable. */
  kv_raw_record_t *mem_rec = NULL;
  if (it->sl_count > 0) {
    pthread_rwlock_rdlock(&it->lsmt->memtable_rwlock);
    wr_sl_ensure_buffered(&it->sl_its[0]);
    pthread_rwlock_unlock(&it->lsmt->memtable_rwlock);
    if (it->sl_its[0].buffered_record.valid) {
      mem_rec = &it->sl_its[0].buffered_record;
    }
  }

  /* Peek from SST list.
   * sst_list_iterator_next advances state. 
   * To strictly compare, we'd need peek on the iterator list too.
   * BUT: standard pattern is:
   * if (!buffered_sst_val) buffered_sst_val = sst_list_next();
   */
  if (!it->buffered_sst_record.valid && it->sst_list.active) {
    kv_raw_record_t *rec = sst_k_iterators_next(&it->sst_list);
    if (rec) {
      // Deep copy to local buffer because set_iterator advances
      it->buffered_sst_record = *rec; 
      it->buffered_sst_record.valid = true;
    }
  }

  kv_raw_record_t *sst_rec = it->buffered_sst_record.valid ?
    &it->buffered_sst_record : NULL;

  if (mem_rec && sst_rec) {
    if (key_compare(mem_rec->key, sst_rec->key) <= 0) {
      result = *mem_rec;
      mem_rec->valid = false; // Advance Memtable
                              // If keys are identical (update), consume SST too to hide old version
      if (key_compare(mem_rec->key, sst_rec->key) == 0) {
        it->buffered_sst_record.valid = false; 
      }
    } else {
      result = *sst_rec;
      it->buffered_sst_record.valid = false; // Advance SST Set
    }
  } else if (mem_rec) {
    result = *mem_rec;
    mem_rec->valid = false;
  } else if (sst_rec) {
    result = *sst_rec;
    it->buffered_sst_record.valid = false;
  } else {
    it->active = false;
  }

  return result;
}

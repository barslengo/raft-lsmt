#ifndef LSMT_H
#define LSMT_H

#include <stdint.h>
#include "skiplist.h"
#include "index.h"
#include "sst.h"

#define LSMT_TYPE_INT 1
#define LSMT_TYPE_STRING 2

/* Callback types for storage events */
typedef void (*lsmt_compaction_callback_t)(void *user_data, uint64_t start_ts, uint64_t end_ts, uint64_t duration_ms,
    uint32_t quantity_merged_tables, uint64_t input_bytes, uint64_t output_bytes, uint8_t level);

typedef void (*lsmt_memtable_flush_callback_t)(void *user_data, uint64_t start_ts, uint64_t end_ts, uint64_t duration_ms,
    uint64_t bytes_flushed);

typedef struct lsmt {
  sst_metadata_t *metadata;
  sl_t *memtable;
  index_t *last_index;
  uint32_t sstable_id; //incremental id for the next sstable
  char *db_path;

  pthread_mutex_t metadata_lock;
  //pthread_mutex_t memtable_lock; //used when swapping memtable.
  pthread_rwlock_t memtable_rwlock;
  
  /* Callbacks for monitoring */
  lsmt_compaction_callback_t compaction_callback;
  lsmt_memtable_flush_callback_t memtable_flush_callback;
  void *callback_user_data;

  pthread_mutex_t thread_pool_mutex;
  pthread_cond_t thread_pool_cond;
  uint16_t active_background_threads;
} lsmt_t;

typedef struct wrapper_sl_it {
  sl_iterator_t sl_it;

  /* Internal use buffer. Owns record allocated data. */
  uint8_t *buffer;
  size_t buffer_cap; 

  kv_raw_record_t buffered_record;
} wrapper_sl_it_t;

typedef struct lsmt_iterator {
  bool active;
  sl_uint128_t start_key;
  sl_uint128_t end_key;
  lsmt_t *lsmt;

  size_t sl_count;
  wrapper_sl_it_t *sl_its;

  sst_k_iterators_t sst_list;

  /* If we peek at the SST list but the Memtable has a smaller key,
   * we return the Memtable record. We must store the SST record 
   * here so it's available for the next comparison.
   */
  kv_raw_record_t buffered_sst_record;
} lsmt_iterator_t;


/*
 * Encode the data into an array of bytes prefixed by the type of the data and it's length.
 * | T | L | L | L | L | DATA |
 */
uint8_t *encode_data(uint8_t type, uint32_t length, void *data);
lsmt_t *lsmt_init(const char *db_path);
void lsmt_flush(lsmt_t *lsmt);
void lsmt_free(lsmt_t *lsmt);
int lsmt_insert(lsmt_t *lsmt, sl_uint128_t key, uint8_t *content, uint32_t size);

lsmt_iterator_t lsmt_iterator_create(lsmt_t *lsmt);
void lsmt_iterator_seek(lsmt_iterator_t *it, sl_uint128_t start, sl_uint128_t end);
void lsmt_iterator_close(lsmt_iterator_t *it);

/* The iterator fetches the records in ascending order within the provided key range. */
kv_raw_record_t lsmt_iterator_next(lsmt_iterator_t *it);

/* Set callbacks for monitoring storage events */
void lsmt_set_compaction_callback(lsmt_t *lsmt, lsmt_compaction_callback_t callback, void *user_data);
void lsmt_set_memtable_flush_callback(lsmt_t *lsmt, lsmt_memtable_flush_callback_t callback, void *user_data);

#endif

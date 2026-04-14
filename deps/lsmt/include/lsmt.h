#ifndef LSMT_H
#define LSMT_H

#include <stdint.h>
#include "skiplist.h"
#include "index.h"
#include "sst.h"

#define LSMT_TYPE_INT 1
#define LSMT_TYPE_STRING 2

typedef struct lsmt {
  sst_metadata_t *metadata;
  sl_t *memtable;
  index_t *last_index;
  uint32_t sstable_id; //incremental id for the next sstable
  char *db_path;

  pthread_mutex_t metadata_lock;
  //pthread_mutex_t memtable_lock; //used when swapping memtable.
  pthread_rwlock_t memtable_rwlock;
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
#endif

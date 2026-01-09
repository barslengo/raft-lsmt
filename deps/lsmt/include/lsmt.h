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
  pthread_mutex_t memtable_lock; //used when swapping memtable.
} lsmt_t;

typedef struct kv_record {
  sl_uint128_t key;
  uint8_t *data;
  uint8_t data_type;
  uint32_t data_len;
  uint64_t record_size;
} kv_record_t;

typedef struct sst_iterator {
  bool active;
  FILE *fp;
  char *io_buff; //read buffer
  sl_uint128_t start_key;
  sl_uint128_t end_key;
} sst_iterator_t;


/* Forward declaration for the internal source wrapper */
//struct merge_source; 
//typedef struct lsmt_iterator lsmt_iterator_t;

/* 
 * Helper to peek/buffer the next record from an underlying source.
 * We need this because we need to compare keys across all SSTables 
 * before deciding which one to advance.
 */
typedef struct merge_source {
  bool active;
  bool has_buffered;
  kv_record_t buffered_record;
  
  /* Underlying iterator type: 0 = memtable, 1 = sst */
  int type; 
  union {
    sl_iterator_t mem_it;
    sst_iterator_t sst_it;
  } iter;
} merge_source_t;

typedef struct lsmt_iterator {
  bool active;
  sl_uint128_t start_key;
  sl_uint128_t end_key;
  lsmt_t *lsmt;

  /* Array of sources (1 Memtable + N SSTables) */
  size_t source_count;
  merge_source_t *sources;
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

lsmt_iterator_t lsmt_iterator_create(lsmt_t *lsmt, sl_uint128_t start_key, sl_uint128_t end_key);
void lsmt_iterator_close(lsmt_iterator_t *it);

/* The iterator fetches the records in ascending order within the provided key range. */
kv_record_t lsmt_iterator_next(lsmt_iterator_t *it);
#endif

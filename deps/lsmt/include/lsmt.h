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
  sl_uint128_t start_key;
  sl_uint128_t end_key;
} sst_iterator_t;


typedef struct lsmt_iterator {
  bool active;
  sl_uint128_t start_key;
  sl_uint128_t end_key;

  lsmt_t *lsmt;
  sst_node_t *sst_list;
  sl_iterator_t memtable_it;
  sst_iterator_t sst_it;
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
kv_record_t lsmt_iterator_next(lsmt_iterator_t *it);


#endif

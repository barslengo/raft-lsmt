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
} lsmt_t;


/*
 * Encode the data into an array of bytes prefixed by the type of the data and it's length.
 * | T | L | L | L | L | DATA |
 */
uint8_t *encode_data(uint8_t type, uint32_t length, void *data);
lsmt_t *lsmt_init(const char *db_path);
void lsmt_flush(lsmt_t *lsmt);
void lsmt_free(lsmt_t *lsmt);
int lsmt_insert(lsmt_t *lsmt, sl_uint128_t key, uint8_t *content, uint32_t size);
uint8_t *lsmt_get(lsmt_t *lsmt, sl_uint128_t key);

#endif

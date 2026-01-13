#ifndef INDEX_H 
#define INDEX_H

#include <stddef.h>
#include <stdint.h>
#include "skiplist.h"

typedef struct index_block {
  sl_uint128_t key;
  uint64_t offset;
} index_block_t;

typedef struct index {
  index_block_t *blocks; 
  size_t capacity;
  size_t current;
} index_t;

index_t *index_init(size_t init_size);
void index_free(index_t *index);
int index_flush(const index_t *index, const char *filename);

int index_add(index_t *index, sl_uint128_t key, uint64_t offset);
index_t *index_load(FILE* stream);
index_t *index_load_from_disk(char *path);
uint64_t index_lookup(const index_t *index, sl_uint128_t key);

#endif

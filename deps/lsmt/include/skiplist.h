#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <stdint.h>
#include "utils.h"

typedef struct node {
  sl_uint128_t key;
  void *content;
  struct node *next;
  struct node *lower_level;
} node_t;

typedef struct sl {
  node_t *top_level;
  node_t *bottom_level;
  size_t size;
  size_t levels;
} sl_t;

int key_compare(sl_uint128_t x, sl_uint128_t y);  
sl_t *sl_init();
void sl_free(sl_t *skiplist);
int sl_insert(sl_t *skiplist, sl_uint128_t key, void *content, size_t content_size);
void *sl_get(sl_t *skiplist, sl_uint128_t key);

#endif

#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <stdatomic.h>
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
  atomic_int refcount;
} sl_t;

typedef struct {
    sl_uint128_t key;
    void *content;
} sl_kv_t;


typedef struct sl_iterator {
  bool active;
  sl_t *sl;
  node_t *current_node;
  sl_uint128_t start_key;
  sl_uint128_t end_key;
} sl_iterator_t;



int key_compare(sl_uint128_t x, sl_uint128_t y);  
sl_t *sl_init();
//void sl_free(sl_t *skiplist);
int sl_insert(sl_t *skiplist, sl_uint128_t key, void *content, size_t content_size);

sl_iterator_t sl_iterator_create(sl_t *sl, sl_uint128_t start_key, sl_uint128_t end_key);
void sl_iterator_close(sl_iterator_t *it);
sl_kv_t sl_iterator_next(sl_iterator_t *it);

void *sl_get(sl_t *skiplist, sl_uint128_t key);

void sl_retain(sl_t *skiplist);
void sl_release(sl_t *skiplist);
#endif

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "skiplist.h"

static sl_uint128_t RESERVED_KEY = { 
  .id = 0,
  .timestamp = 0
};
/*
 * This is a really trivial implementation of a skiplist. It's not optimized at all and probably has some bugs.
 * Next step maybe to update the struct in order to minimize the cache misses which are really high due to the sparsity of the nodes.
 * Use perf for analyize performance
 * perf stat -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses ./program
*/

int key_compare(sl_uint128_t x, sl_uint128_t y) {
  if (x.id < y.id) return -1;
  if (x.id > y.id) return 1;

  if (x.timestamp < y.timestamp) return -1;
  if (x.timestamp > y.timestamp) return 1;

  return 0;
}

static void node_free(node_t *node) {
  if (node == NULL) return;
  if (node->lower_level == NULL && node->content != NULL) free(node->content);
  free(node);
  node = NULL;
}

static void sl_free(sl_t *skiplist) {
  node_t *lvl_head = skiplist->top_level;

  while (lvl_head != NULL) {
    node_t *current = lvl_head->next;
    while (current != NULL) {
      node_t *tmp = current->next;
      node_free(current);
      current = tmp;
    }
    lvl_head = lvl_head->lower_level;
  }

  while (skiplist->top_level != NULL ) {
    node_t *tmp = skiplist->top_level->lower_level;
    node_free(skiplist->top_level);
    skiplist->top_level = tmp;
  }

  free(skiplist);
  skiplist = NULL;
}

void sl_retain(sl_t *skiplist) {
  if (skiplist) atomic_fetch_add(&skiplist->refcount, 1);
}

void sl_release(sl_t *skiplist) {
  if (!skiplist) return;

  if (atomic_fetch_sub(&skiplist->refcount, 1) > 1) return;

  sl_free(skiplist);
}

static node_t *new_node(sl_uint128_t key, void *content) {
  node_t *node = malloc(sizeof(node_t));
  if (node == NULL) {
    printf("Error: failed node allocation");
    exit(1);
  }
 
  node->key = key;
  node->content = content;
  node->next = NULL;
  node->lower_level = NULL;

  return node;
}

/*
 * Allocate a new node "cpy" pointing to the given node "node" and promote it to the upper level:
 *
 * |. . . cpy . . . |
 * |      ^         |
 * |      |         |
 * |      v         |
 * |. . . node . . .|
 *
 */
static node_t *new_promoted_node(node_t *node) {
  node_t *cpy = malloc(sizeof(node_t));
  cpy->key = node->key;
  cpy->content = node->content;
  cpy->next = NULL;
  cpy->lower_level = node;

  return cpy;
}


/* Allocate and initialize a new skiplist. Every level begins with a sentinel node which has always the same key.
 * To prevent any issues the key assigned to the sentinel node is reserved, so it can't be used inside the skiplist.
 */
sl_t* sl_init() {
  sl_t *skiplist = malloc(sizeof(sl_t));
  node_t *head = new_node(RESERVED_KEY, NULL);
  skiplist->size = 0;
  skiplist->levels = 1;
  skiplist->top_level = head;
  skiplist->bottom_level = head;
  atomic_init(&skiplist->refcount, 1);

  skiplist->min_key.id = UINT64_MAX;
  skiplist->min_key.timestamp = UINT64_MAX;

  skiplist->max_key.id = 0;
  skiplist->max_key.timestamp = 0;
  return skiplist;
}

/* 
 * Create the iterator for the skiplist.
 */
sl_iterator_t sl_iterator_create(sl_t *skiplist) {
  sl_iterator_t it = {0};
  it.active = false;
  if (skiplist) {
    sl_retain(skiplist);
    it.sl = skiplist;
    it.start_key = skiplist->min_key;
    it.end_key = skiplist->max_key;
  }
  return it;
}

void sl_iterator_close(sl_iterator_t *it) {
  if (it && it->sl) {
    sl_release(it->sl);
    it->sl = NULL;
  }

  if (it) {
    it->active = false;
  }
}

bool sl_iterator_seek(sl_iterator_t *it, sl_uint128_t start_key, sl_uint128_t end_key) {
  if (!it ||!it->sl) return false; 

  if (key_compare(it->sl->max_key, start_key) < 0 ||
      key_compare(it->sl->min_key, end_key) > 0) {
    it->active = false;
    return false;
  }

  it->start_key = start_key;
  it->end_key = end_key;
  it->active = true;

  /* Traverse down to the bottom level to find the node strictly before start_key */
  node_t *current = it->sl->top_level;
  while (current != NULL) {
    /* Move right while the next key is strictly less than start_key */
    while (current->next != NULL && key_compare(current->next->key, start_key) < 0) {
      current = current->next;
    }

    /* Drop down a level, or stop if we are at the bottom */
    if (current->lower_level != NULL) {
      current = current->lower_level;
    }
    else {
      break; 
    }
  }

  /* current is now the predecessor (or sentinel). Move to the first potential candidate. */
  if (current != NULL) {
    current = current->next;
  }

  it->current_node = current;
  return true;
}

/* Iterates until the current key is greater than the 'end_key'
 * provided by the iterator.
 * The ownership of the returned record 'content' is still of the skiplist.
 */
sl_kv_t sl_iterator_next(sl_iterator_t *it) {
  sl_kv_t record = {0};
  if (!it || !it->active) return record;

  if (it->current_node == NULL || 
      key_compare(it->current_node->key, it->end_key) > 0) {
    it->active = false;
    //sl_iterator_close(it);
    return record;
  }

  record.key = it->current_node->key;
  record.content = it->current_node->content;
  it->current_node = it->current_node->next;

  return record;
}


void* sl_get(sl_t *skiplist, sl_uint128_t key) {
  if (skiplist == NULL) return NULL;

  node_t *prev = skiplist->top_level;
  node_t *current = prev->next;
  node_t *lvl_head = skiplist->top_level;

  size_t level = skiplist->levels-1;

  while (lvl_head != NULL && current != NULL) {
    prev = NULL;
    //printf("\nlevel %ld) ------------------------------------------------ \n", level);

    //iterate the linkedlist in the current level
    while (current != NULL && key_compare(current->key, key) < 0) {
      prev = current;
      current = current->next;

      //printf("(key=%ld -> key=%ld) \n", prev->key.id, current->key.id);
    }

    //found
    if (current != NULL && key_compare(current->key, key) == 0) {
      return current->content;
    }

    /* Means that the current list has no keys less than the one i'm looking for,
     * so i just skip to the next */
    if (prev == NULL) {
      if (lvl_head->lower_level != NULL) {
        current = lvl_head->lower_level->next;
      }
    }
    else {
      //move to previous node and drop down by one level
      current = prev->lower_level;
    }
    lvl_head = lvl_head->lower_level;
    level--;
  }

  //Not found.
  return NULL;
}

/* Insert a new node into an existing skiplist.
 * They key 0 is reserved, so this function will ignore any request for inserting a node with key=0.
 * Returns 0 if the insert is performed correctly. */
int sl_insert(sl_t *skiplist, sl_uint128_t key, void *content, size_t content_size) {
  if (key.id == RESERVED_KEY.id) return -1;
  if (skiplist == NULL || skiplist->top_level == NULL) {
    printf("Invalid skiplist");
    return -1;
  }

  if (key_compare(key, skiplist->min_key) < 0) {
    skiplist->min_key = key;
  }
  if (key_compare(key, skiplist->max_key) > 0) {
    skiplist->max_key = key;
  }

  //search for bottom level position for the new node
  node_t *prev = skiplist->top_level;
  node_t *lvl_head = skiplist->top_level;

  size_t levels = skiplist->levels;
  long current_level = levels-1;

  node_t *current = prev->next;
  node_t *promotions[levels];
  while (prev != NULL) {
    //iterate the linkedlist in the current level
    while (current != NULL && key_compare(current->key, key) < 0) {
      prev = current;
      current = current->next;
    }

    //the key already exists!
    if (current != NULL && key_compare(current->key, key) == 0) {
      return -1;
    }

    //move to previous node and drop down by one level
    promotions[current_level--] = prev;
    current = prev->lower_level;
    lvl_head = lvl_head->lower_level;
    prev = lvl_head;
  }

  void *copied_content = malloc(content_size);
  if (copied_content == NULL) {
    printf("Error: failed content allocation");
    exit(1);
  }
  memcpy(copied_content, content, content_size);
  node_t *node_to_insert = new_node(key, copied_content);

  /* traverse the lowest linked-list to find the position for the new node. */
  prev = promotions[0];
  current = prev->next;
  while (current != NULL && key_compare(current->key, key) < 0) {
    prev = current;
    current = current->next;
  }

  //i'm at the end of the bottom linked-list
  if (current == NULL) {
    prev->next = node_to_insert;
  }
  else {
    /* current key is higher than the one i'm inserting. so i need to add the new node
       * between prev and current.
       */
    prev->next = node_to_insert;
    node_to_insert->next = current;
  }

  //Flip coins until i get tail: keep promote the new node on the next level.
  node_t *last_promoted = node_to_insert;
  node_t *level_head;
  size_t promotion_level = 1;

  while (rand() & 1) {
    node_t *promoted_node = new_promoted_node(last_promoted);
    if (promotion_level < skiplist->levels) {
      promoted_node->next = promotions[promotion_level]->next;
      promotions[promotion_level]->next = promoted_node;    
    }
    else {
      /*
         * The list is out of levels. Since i already have the promoted node pointing to the lower level,
         * the new list is only composed by that node, so i don't have to do anything else but incrementing the
         * total number of levels.
        */
      level_head = new_promoted_node(skiplist->top_level);
      level_head->next = promoted_node;

      skiplist->top_level = level_head;
      skiplist->levels++;
    }

    last_promoted = promoted_node;
    promotion_level++;
  }

  skiplist->size = skiplist->size + sizeof(node_t) + content_size;
  return 0;
}

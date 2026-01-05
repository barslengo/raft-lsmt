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

  return skiplist;
}

/*
 * Retrieves all key/value pairs in the range [start_key, end_key] (inclusive).
 * 
 * @param skiplist   Pointer to the skiplist instance.
 * @param start_key  The lower bound of the range.
 * @param end_key    The upper bound of the range.
 * @param result     A pointer to a (sl_kv_t*) pointer. This function will allocate 
 *                   memory for the array of results and set this pointer. 
 *                   The caller is responsible for freeing this memory.
 * 
 * @return uint32_t  The number of records found.
 */
uint32_t sl_get_range(sl_t *skiplist, sl_uint128_t start_key, sl_uint128_t end_key, sl_kv_t **result) {
    if (skiplist == NULL || result == NULL) {
        return 0;
    }

    node_t *current = skiplist->top_level;

    /* Traverse down to the bottom level to find the node strictly before start_key */
    while (current != NULL) {
        /* Move right while the next key is strictly less than start_key */
        while (current->next != NULL && key_compare(current->next->key, start_key) < 0) {
            current = current->next;
        }

        /* Drop down a level, or stop if we are at the bottom */
        if (current->lower_level != NULL) {
            current = current->lower_level;
        } else {
            break; 
        }
    }

    /* current is now the predecessor (or sentinel). Move to the first potential candidate. */
    if (current != NULL) {
        current = current->next;
    }

    /* Initialize Result Array */
    size_t capacity = 16;
    size_t count = 0;
    sl_kv_t *arr = malloc(capacity * sizeof(sl_kv_t));
    if (arr == NULL) {
        return 0; 
    }

    /* Iterate forward at the bottom level */
    while (current != NULL) {
        /* If current key is greater than end_key, we are done */
        if (key_compare(current->key, end_key) > 0) {
            break;
        }

        /* Resize array if necessary */
        if (count >= capacity) {
            capacity *= 2;
            sl_kv_t *tmp = realloc(arr, capacity * sizeof(sl_kv_t));
            if (tmp == NULL) {
                free(arr); 
                *result = NULL;
                return 0;
            }
            arr = tmp;
        }

        /* Add record to result */
        arr[count].key = current->key;
        arr[count].content = current->content;
        count++;

        current = current->next;
    }

    /* Assign the allocated array to the output parameter */
    *result = arr;
    return (uint32_t)count;
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

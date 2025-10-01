#ifndef QUEUE_H
#define QUEUE_H

#include <stdint.h>
#include <stdlib.h>

typedef struct queue {
  void **data;
  uint32_t capacity;
  uint32_t front;
  uint32_t rear;
  uint32_t size;
} queue_t;

queue_t *queue_init();
void queue_free(queue_t *queue); 
void queue_clean(queue_t *queue);
int enqueue(queue_t *queue, void *content);
void *dequeue(queue_t *queue);

#endif

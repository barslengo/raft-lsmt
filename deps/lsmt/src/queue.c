#include "queue.h"

#define QUEUE_CAPACITY (4 * 1024)

void queue_clean(queue_t *queue) {
  free(queue->data);
  queue->data = NULL;
  queue->front = 0;
  queue->rear = 0;
  queue->size = 0;

  return;
}

queue_t *queue_init() {
  queue_t *queue = malloc(sizeof(queue_t));
  queue->front = 0;
  queue->rear = 0;
  queue->size = 0;
  queue->capacity = QUEUE_CAPACITY;
  queue->data = malloc(sizeof(void *) * queue->capacity); 

  return queue;
}

void queue_free(queue_t *queue) {
  if (!queue) return;
  if (queue->data) free(queue->data);
  free(queue);
}

int enqueue(queue_t *queue, void *data) {
  if (!queue || queue->size == queue->capacity) return 0;
  
  queue->data[queue->rear] = data;
  queue->rear = (queue->rear + 1) % queue->capacity;
  queue->size++;
  return 1;
}

void *dequeue(queue_t *queue) {
  if (!queue || queue->size == 0) return NULL;

  void *data = queue->data[queue->front];
  queue->front = (queue->front + 1) % queue->capacity;
  queue->size--;

  return data;
}


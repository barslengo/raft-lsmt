#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "index.h"

#define DEFAULT_CAPACITY 1024

static int ensure_capacity(index_t *index, size_t needed) {
  if (!index || needed <= index->capacity) return 0;

  size_t new_cap = index->capacity == 0 ? DEFAULT_CAPACITY : index->capacity * 2;
  while (new_cap < needed) new_cap *= 2;

  index_block_t *tmp = realloc(index->blocks, sizeof(index_block_t) * new_cap);
  if (!tmp) {
    perror("Failed to resize index");
    return -1;
  }
  index->blocks = tmp;
  index->capacity = new_cap;
  return 0;
}

index_t *index_init(size_t init_size) {
  index_t *index = malloc(sizeof(index_t));
  if (!index) return NULL;

  index->blocks = NULL;
  index->capacity = 0;
  index->current = 0;

  if (init_size > 0) {
    ensure_capacity(index, init_size);
  }
  return index;
}

void index_free(index_t *index) {
  if (index) {
    free(index->blocks);
    free(index);
  }
}

int index_add(index_t *index, sl_uint128_t key, uint64_t offset) {
  if (!index || ensure_capacity(index, index->current + 1) != 0) {
    return -1;
  }

  index->blocks[index->current].key = key;
  index->blocks[index->current].offset = offset;
  index->current++;

  return 0;
}

/* Dump the index data into disk */
int index_flush(const index_t *index, const char *filename) {
  if (index == NULL) return 0;
  FILE *fp = fopen(filename, "wb");

  uint32_t buf_size = sizeof(index_block_t);
  for (size_t i = 0; i < index->current; i++) {
    uint8_t buf[buf_size];
    uint32_t off = 0;

    memcpy(buf + off, &index->blocks[i].key, sizeof(index->blocks[i].key));
    off += sizeof(index->blocks[i].key);
    memcpy(buf + off, &index->blocks[i].offset, sizeof(index->blocks[i].offset));
    off += sizeof(index->blocks[i].offset);

    if (fwrite(buf, off, 1, fp) != 1) {
      perror("Error on writing index record.");
      exit(1);
      return -1;
    }
  }

  fclose(fp);
  return 0;
}

index_t *index_load(FILE* stream) {
  index_t *index = index_init(DEFAULT_CAPACITY);
  index_block_t ib;
  const size_t rec_size = sizeof(ib.key) + sizeof(ib.offset);
  uint8_t buf[rec_size];

  while (fread(buf, 1, rec_size, stream) == rec_size) {
    memcpy(&ib.key, buf, sizeof(ib.key));
    memcpy(&ib.offset, buf + sizeof(ib.key), sizeof(ib.offset));
    index_add(index, ib.key, ib.offset);
  }
  return index;
}

index_t *index_load_from_disk(char *path) {
  FILE *fp = fopen(path, "rb");
  if (!fp) {
    perror("[INDEX LOAD] failed opening index file");
    exit(1);
  }

  index_t *index = index_load(fp);
  fclose(fp);
  return index;
}

uint64_t index_lookup(const index_t *index, sl_uint128_t key) {
  if (index == NULL || index->current == 0) return 0;

  int64_t start = 0;
  int64_t end = index->current-1;
  size_t best_idx = 0;

  while (start <= end) {
    int64_t half = start + ((end - start) / 2); 
    int cmp = key_compare(index->blocks[half].key, key);

    if (cmp < 0) {
      best_idx = half;
      start = half + 1;
    }
    else if (cmp > 0) {
      end = half - 1;
    }
    else {
      best_idx = half;
      break;
    }
  }

  return index->blocks[best_idx].offset;
}

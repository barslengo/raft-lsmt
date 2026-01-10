#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "index.h"

#define DEFAULT_CAPACITY 1024

index_t *index_init(size_t init_size) {
  index_t *index = malloc(sizeof(index_t));
  index->blocks = malloc(sizeof(index_block_t) * init_size);
  index->capacity = init_size;
  index->current = 0;

  return index;
}

void index_free(index_t *index) {
  if (index == NULL) return;
  if (index->blocks != NULL) free(index->blocks);
  free(index);
  return;
}

index_t *index_add(index_t *index, sl_uint128_t key, uint64_t offset) {
  if (index == NULL) {
    index = index_init(DEFAULT_CAPACITY);
  }
  if(index->current >= index->capacity) {
    index->capacity *= 2;
    index->blocks = realloc(index->blocks, sizeof(index_block_t) * index->capacity);
  }

  index->blocks[index->current].key = key;
  index->blocks[index->current].offset = offset;
  index->current++;

  return index;
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
  index_t *index = NULL;
  index_block_t ib;
  const size_t rec_size = sizeof(ib.key) + sizeof(ib.offset);
  uint8_t buf[rec_size];

  while (fread(buf, 1, rec_size, stream) == rec_size) {
    memcpy(&ib.key, buf, sizeof(ib.key));
    memcpy(&ib.offset, buf + sizeof(ib.key), sizeof(ib.offset));
    index = index_add(index, ib.key, ib.offset);
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
  size_t start = 0;
  size_t end = index->current-1;
  size_t best_idx = 0;
  index_block_t *blocks = index->blocks;

  while (start <= end) {
    size_t half = start + ((end - start) / 2); 
    if (key_compare(blocks[half].key, key) < 0) {
      best_idx = half;
      start = half + 1;
    }
    else if (key_compare(blocks[half].key, key) > 0) {
      if (half - 1 == SIZE_MAX) break;
      end = half - 1;
    }
    else {
      best_idx = half;
      break;
    }
  }

  return blocks[best_idx].offset;
}

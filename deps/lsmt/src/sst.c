#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <unistd.h>
#include "utils.h"
#include "queue.h"
#include "sst.h"

static int tier_init(sst_metadata_t *sst_meta) {
  for (uint8_t i = 0; i < 64; i++) {
    sst_meta->tier[i] = NULL; 
    sst_meta->tier_size[i] = 0.0;
  }
  return 0;
}

static int tier_free(sst_metadata_t *sst_meta, uint8_t max_tier) {
  for (uint8_t i = 0; i < max_tier; i++) {
    if (sst_meta->tier[i] != NULL) queue_free(sst_meta->tier[i]);
  }
  return 0;
}

void sst_metadata_record_retain(sst_metadata_record_t *meta) {
  if (meta) {
    atomic_fetch_add(&meta->refcount, 1);
  }
}

void sst_metadata_record_release(sst_metadata_record_t *meta) {
  if (!meta) return;

  if (atomic_fetch_sub(&meta->refcount, 1) == 1) {
    if (meta->cached_index) {
      index_free(meta->cached_index);
      meta->cached_index = NULL;
    }

    if (meta->fd != -1) {
      close(meta->fd);
      meta->fd = -1;
    }

    if (meta->old) {
      remove(meta->sstable_filename);
      remove(meta->sst_index_filename);
    }

    free(meta);
  }
}

static sst_node_t *new_node(sst_metadata_record_t content) {
  sst_node_t *node = malloc(sizeof(sst_node_t));
  node->content = malloc(sizeof(sst_metadata_record_t));

  content.cached_index = NULL;
  content.old = false;
  content.fd = -1;
  *node->content = content; 
  atomic_init(&node->content->refcount, 1);

  node->next = NULL;
  node->prev = NULL;
  return node;
}

static void node_free(sst_node_t *node) {
  if (!node) return;
  if (node->content) {
    sst_metadata_record_release(node->content);
  }
  free(node);
}

static void list_free(sst_node_t *head) {
  while (head != NULL) {
    sst_node_t *next = head->next;
    node_free(head);
    head = next;
  }
  return;
}

static sst_node_t *node_add(sst_node_t *list, sst_node_t* node) {
  if (!list) return node; 

  node->next = list;
  node->next->prev = node;

  return node;
}

static sst_node_t *node_remove(sst_node_t *list, sst_node_t *node) {
  /* If node is the header. */
  if (node == list) {
    sst_node_t *new_head = node->next;

    if (new_head != NULL) {
      new_head->prev = NULL;
    }

    node_free(node);
    return new_head;
  }

  /* Removing middle or tail node. */
  if (node->prev) {
    node->prev->next = node->next;
  }

  if (node->next) {
    node->next->prev = node->prev;
  }

  node_free(node);
  return list;
}

sst_metadata_t *sst_metadata_init(const char *path) {
  sst_metadata_t *meta = malloc(sizeof(sst_metadata_t));
  snprintf(meta->disk_path, sizeof(meta->disk_path), "%s/.lsmt_metadata", path); 
  meta->version = 1;
  meta->highest_tier = 0;

  meta->list = NULL;
  tier_init(meta);

  return meta;
}

sst_metadata_record_t create_sst_metadata(uint64_t id, uint32_t level,
                                          uint64_t size, sl_uint128_t min_key,
                                          sl_uint128_t max_key, char* sst_path,
                                          char *index_path) {

  sst_metadata_record_t metadata = (sst_metadata_record_t){
    .sstable_filename = { 0 },
    .id = id, 
    .level = level,
    .created_at = get_unix_epoch(), 
    .total_bytes = size,
    .min_key = min_key,
    .max_key = max_key
  };
  metadata.cached_index = NULL;
  metadata.fd = -1;

  strncpy(metadata.sstable_filename, sst_path, 128 - 1); 
  metadata.sstable_filename[128-1] = '\0';

  strncpy(metadata.sst_index_filename, index_path, 128 - 1); 
  metadata.sst_index_filename[128-1] = '\0'; 

  return metadata;
}

sst_metadata_t *sst_metadata_load(const char *path) {
  printf("METHOD NOT IMPLEMENTED");
  exit(1);
}

int sst_metadata_free(sst_metadata_t *sst_meta) {
  if (!sst_meta) return 0;

  //if (sst_meta->disk_path) free(sst_meta->disk_path);
  if (sst_meta->list) list_free(sst_meta->list);
  tier_free(sst_meta, 64);
  return 0;
}

int sst_metadata_flush(sst_metadata_t *sst_meta) { 
  char temp_path[256];
  snprintf(temp_path, sizeof(temp_path), "%s.tmp", sst_meta->disk_path);

  FILE *fp = fopen(temp_path, "w");
  if (!fp) {
    perror("FAILED FLUSHING METADATA");
    exit(1);
  }

  fwrite(&sst_meta->version, sizeof(sst_meta->version), 1, fp);
  fwrite(&sst_meta->highest_tier, sizeof(sst_meta->highest_tier), 1, fp);

  sst_node_t *node = sst_meta->list;
  size_t meta_max_size = sizeof(sst_metadata_record_t);
  while (node != NULL) {
    uint8_t buf[meta_max_size];
    uint32_t off = 0;
    sst_metadata_record_t data = *node->content;

    memcpy(buf + off, data.sstable_filename, sizeof(data.sstable_filename));
    off += sizeof(data.sstable_filename);
    memcpy(buf + off, data.sst_index_filename, sizeof(data.sst_index_filename));
    off += sizeof(data.sst_index_filename);
    memcpy(buf + off, &data.id, sizeof(data.id));
    off += sizeof(data.id);
    memcpy(buf + off, &data.level, sizeof(data.level));
    off += sizeof(data.level);
    memcpy(buf + off, &data.created_at, sizeof(data.created_at));
    off += sizeof(data.created_at);
    memcpy(buf + off, &data.total_bytes, sizeof(data.total_bytes));
    off += sizeof(data.total_bytes);
    memcpy(buf + off, &data.min_key, sizeof(data.min_key));
    off += sizeof(data.min_key);
    memcpy(buf + off, &data.max_key, sizeof(data.max_key));
    off += sizeof(data.max_key);

    fwrite(buf, off, 1, fp);
    node = node->next;
  }

  if (fflush(fp) != 0) {
    perror("Failed to flush buffer");
    fclose(fp);
    exit(1);
  }

  if (fsync(fileno(fp)) != 0) {
    perror("Failed to sync to disk");
    fclose(fp);
    exit(1);
  }

  fclose(fp);

  if (rename(temp_path, sst_meta->disk_path) != 0) {
    perror("Failed to rename the file");
    exit(1);
  }
  return 0;
}

int sst_metadata_add(sst_metadata_t *sst_meta, sst_metadata_record_t record) {
  sst_node_t *node = new_node(record);
  sst_meta->list = node_add(sst_meta->list, node);

  if (sst_meta->tier[record.level] == NULL) {
    sst_meta->tier[record.level] = queue_init();
    sst_meta->highest_tier++;
  }

  enqueue(sst_meta->tier[record.level], node);
  sst_meta->tier_size[record.level] += record.total_bytes;

  return 0;
}

/*The caller will own the metadata_record reference. */
sst_metadata_record_t *sst_metadata_pop(sst_metadata_t *sst_meta, uint8_t tier) {
  sst_node_t *node = (sst_node_t*)dequeue(sst_meta->tier[tier]);

  if (!node) {
    return NULL;
  }

  sst_metadata_record_t *record = node->content;

  /* Detach content form the node so node_free doesnt decrement the refcount. */
  node->content = NULL;
  sst_meta->list = node_remove(sst_meta->list, node);

  sst_meta->tier_size[record->level] = fmax(0, sst_meta->tier_size[record->level] - record->total_bytes);

  return record;
}

uint32_t sst_count(sst_metadata_t *sst_meta, uint8_t tier) {
  return sst_meta->tier[tier]->size;
}

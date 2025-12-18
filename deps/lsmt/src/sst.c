#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include "utils.h"
#include "queue.h"
#include "sst.h"
/*
typedef struct sst_node {
  sst_metadata_record_t *content;
  struct sst_node *next;
  struct sst_node *prev;
} sst_node_t;
*/
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

static sst_node_t *new_node(sst_metadata_record_t content) {
  sst_node_t *node = malloc(sizeof(sst_node_t));
  node->content = malloc(sizeof(sst_metadata_record_t));
  *node->content = content; 

  node->next = NULL;
  node->prev = NULL;
  return node;
}

static void node_free(sst_node_t *node) {
  if (!node) return;
  if (node->content) free(node->content);
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
    node_free(node);
    return NULL;
  }

  /*If prev node is the header, then i must update it. */
  if (node->prev == list) {
    list->next = node->next;
  }
  else {
    node->prev->next = node->next;
  }

  if (node->next != NULL) {
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
  while (node != NULL) {
    sst_metadata_record_t data = *node->content;
    fwrite(data.sstable_filename, sizeof(data.sstable_filename), 1, fp);
    fwrite(data.sst_index_filename, sizeof(data.sst_index_filename), 1, fp);
    fwrite(&data.id, sizeof(data.id), 1, fp);
    fwrite(&data.level, sizeof(data.level), 1, fp);
    fwrite(&data.created_at, sizeof(data.created_at), 1, fp);
    fwrite(&data.total_bytes, sizeof(data.total_bytes), 1, fp);
    fwrite(&data.min_key, sizeof(data.min_key), 1, fp);
    fwrite(&data.max_key, sizeof(data.max_key), 1, fp);
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

  /*
  // header metadata
  fprintf(fp, "version: %d\n", meta->version); 
  fprintf(fp, "last_dump_offset: %d\n", meta->last_dump_offset);
  fprintf(fp, "highest_tier: %d\n", meta->highest_tier);

  // SSTable metadata 
  fprintf(fp, "--- SSTables ---\n");
  dll_node_t *node = meta->sst_metadata.data->head;
  while (node != NULL) {
    sstable_metadata_record_t data = *node->content;
    fprintf(fp, "[SSTable]\n");
    fprintf(fp, "file_path: %s\n", data.sstable_filename);
    fprintf(fp, "index_path: %s\n", data.sst_index_filename);
    fprintf(fp, "id: %d\n", data.id);
    fprintf(fp, "level: %d\n", data.level);
    fprintf(fp, "created_at: %ld\n", data.created_at);
    fprintf(fp, "total_bytes: %ld\n", data.total_bytes);
    fprintf(fp, "min_key: %lu%ld\n", data.min_key.id, data.min_key.timestamp);
    fprintf(fp, "max_key: %lu%ld\n", data.max_key.id, data.max_key.timestamp);
    fprintf(fp, "\n");
    node = node->next;
  }
  */
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

sst_metadata_record_t sst_metadata_pop(sst_metadata_t *sst_meta, uint8_t tier) {
  sst_node_t *node = (sst_node_t*)dequeue(sst_meta->tier[tier]);

  if (!node) {
    /*this should never happen.. */
    return (sst_metadata_record_t){0};
  }

  sst_metadata_record_t record = *(node->content);

  sst_meta->list = node_remove(sst_meta->list, node);
  sst_meta->tier_size[record.level] = fmax(0, sst_meta->tier_size[record.level] - record.total_bytes);

  return record;
}

uint32_t get_sst_count(sst_metadata_t *sst_meta, uint8_t tier) {
  return sst_meta->tier[tier]->size;
}

#ifndef SST_H 
#define SST_H

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include "utils.h"

typedef struct queue queue_t;

typedef struct sst_metadata_record {
  char sstable_filename[256];
  char sst_index_filename[256];
  uint32_t id;
  uint32_t level;
  uint64_t created_at;
  uint64_t total_bytes;
  sl_uint128_t min_key;
  sl_uint128_t max_key;
} sst_metadata_record_t;

typedef struct sst_node {
  sst_metadata_record_t *content;
  struct sst_node *next;
  struct sst_node *prev;
} sst_node_t;

typedef struct sst_metadata {
  char disk_path[512];
  uint8_t version;
  /* bytes offset on disk to the position of the start of the last write.
   * This way i can easily load metadata of the last N sstables. 
   */
  
  /* highest sstable tier */
  uint8_t highest_tier;

  /* SSTables metadata list, ordered by insertion. */
  sst_node_t *list;

  /* SSTables metadata grouped by tier. */
  queue_t *tier[64];
  double tier_size[64];
} sst_metadata_t;

sst_metadata_t *sst_metadata_init(const char *path);
sst_metadata_t *sst_metadata_load(const char *path);
int sst_metadata_free(sst_metadata_t *sst_meta);
int sst_metadata_flush(sst_metadata_t *sst_meta);
int sst_metadata_add(sst_metadata_t *sst_meta, sst_metadata_record_t record); 
sst_metadata_record_t sst_metadata_pop(sst_metadata_t *sst_meta, uint8_t tier);
sst_metadata_record_t create_sst_metadata(uint64_t id, uint32_t level, uint64_t size, sl_uint128_t min_key, sl_uint128_t max_key, char* sst_path, char *index_path);
sst_metadata_record_t sst_metadata_lookup(sst_metadata_t *sst_meta, sl_uint128_t key);

#endif


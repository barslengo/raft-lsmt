#ifndef SST_H 
#define SST_H

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "utils.h"
#include "index.h"

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
  bool old;
  index_t *cached_index;
  pthread_mutex_t index_lock;
  atomic_int refcount;
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


typedef struct kv_raw_record {
  sl_uint128_t key;

  /* Points to the internal buffer of the iterator. Do NOT free. */
  uint8_t *raw_data;

  /* Total size of raw_data in bytes. */
  size_t total_size;

  /* 
   * True if the record was read successfully.
   * False if EOF, Error, or Out-of-Range.
   */
  bool valid;
} kv_raw_record_t;

typedef struct sst_iterator {
  bool active;
  uint64_t current_offset;
  sst_metadata_record_t *meta;
  FILE *fp;
  sl_uint128_t start_key;
  sl_uint128_t end_key;

  /* Internal use buffer. Owns record allocated data. */
  uint8_t *buffer;
  size_t buffer_cap;

  /* The parsed result pointing to 'buffer' */
  kv_raw_record_t buffered_record;

  /* If true, the file pointer is currently pointing at the body of
   * this record.
   */
  bool has_pending_header;

  sl_uint128_t pending_key;
  uint32_t pending_body_len;
  uint8_t pending_raw_header[21];
} sst_iterator_t;


/*
 * Handles multiple (count) SST iterators.
 */
typedef struct sst_k_iterators {
  sst_iterator_t **iterators;
  size_t count;
  bool active;

  size_t next_lazy_idx;
  sl_uint128_t seek_start;
  sl_uint128_t seek_end;

  kv_raw_record_t current_record;
} sst_k_iterators_t;


sst_metadata_t *sst_metadata_init(const char *path);
sst_metadata_t *sst_metadata_load(const char *path);

void sst_metadata_record_release(sst_metadata_record_t *meta);
void sst_metadata_record_retain(sst_metadata_record_t *meta);

int sst_metadata_free(sst_metadata_t *sst_meta);
//int sst_metadata_flush(sst_metadata_t *sst_meta);
int sst_metadata_copy_records(sst_metadata_t *sst_meta, sst_metadata_record_t **records_out, int *count_out, uint8_t *version_out, uint8_t *highest_tier_out);
int sst_metadata_write_records(sst_metadata_t *sst_meta, sst_metadata_record_t *records, int count, uint8_t version, uint8_t highest_tier);
int sst_metadata_add(sst_metadata_t *sst_meta, sst_metadata_record_t record); 
sst_metadata_record_t *sst_metadata_pop(sst_metadata_t *sst_meta, uint8_t tier);
sst_metadata_record_t create_sst_metadata(uint64_t id, uint32_t level, uint64_t size, sl_uint128_t min_key, sl_uint128_t max_key, char* sst_path, char *index_path);
sst_metadata_record_t sst_metadata_lookup(sst_metadata_t *sst_meta, sl_uint128_t key);
uint32_t sst_count(sst_metadata_t *sst_meta, uint8_t tier);

sst_iterator_t *sst_iterator_create(sst_metadata_record_t *sst);
bool sst_iterator_seek(sst_iterator_t *it, sl_uint128_t start, sl_uint128_t end);
void sst_iterator_free(sst_iterator_t *it);

sst_k_iterators_t sst_k_iterators_create(sst_metadata_record_t **records, size_t count);
void sst_k_iterators_close(sst_k_iterators_t *it);
void sst_k_iterators_seek(sst_k_iterators_t *it, sl_uint128_t start, sl_uint128_t end);
kv_raw_record_t *sst_k_iterators_next(sst_k_iterators_t *it);

#endif


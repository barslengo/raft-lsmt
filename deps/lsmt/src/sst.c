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

    pthread_mutex_destroy(&meta->index_lock);

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
  //content.fp = NULL;
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
  pthread_mutex_init(&metadata.index_lock, NULL);

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

int sst_metadata_copy_records(sst_metadata_t *sst_meta, sst_metadata_record_t **records_out, int *count_out, uint8_t *version_out, uint8_t *highest_tier_out) {
  if (sst_meta == NULL) return -1;
  int count = 0;
  sst_node_t *node = sst_meta->list;
  while (node != NULL) {
    count++;
    node = node->next;
  }

  sst_metadata_record_t *records = NULL;
  if (count > 0) {
    records = malloc(sizeof(sst_metadata_record_t) * count);
    if (records == NULL) {
      return -1;
    }
    node = sst_meta->list;
    for (int i = 0; i < count; i++) {
      records[i] = *node->content;
      node = node->next;
    }
  }

  *records_out = records;
  *count_out = count;
  *version_out = sst_meta->version;
  *highest_tier_out = sst_meta->highest_tier;
  return 0;
}

int sst_metadata_write_records(sst_metadata_t *sst_meta, sst_metadata_record_t *records, int count, uint8_t version, uint8_t highest_tier) {
  if (sst_meta == NULL) return -1;
  char temp_path[256];
  snprintf(temp_path, sizeof(temp_path), "%s.tmp", sst_meta->disk_path);

  FILE *fp = fopen(temp_path, "w");
  if (!fp) {
    perror("FAILED WRITING METADATA RECORDS");
    return -1;
  }

  fwrite(&version, sizeof(version), 1, fp);
  fwrite(&highest_tier, sizeof(highest_tier), 1, fp);

  size_t meta_max_size = sizeof(sst_metadata_record_t);
  for (int i = 0; i < count; i++) {
    uint8_t buf[meta_max_size];
    uint32_t off = 0;
    sst_metadata_record_t data = records[i];

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

    if (fwrite(buf, off, 1, fp) != 1) {
      perror("Error writing record in sst_metadata_write_records");
      fclose(fp);
      return -1;
    }
  }

  if (fflush(fp) != 0) {
    perror("Failed to flush buffer in sst_metadata_write_records");
    fclose(fp);
    return -1;
  }

  if (fsync(fileno(fp)) != 0) {
    perror("Failed to sync in sst_metadata_write_records");
    fclose(fp);
    return -1;
  }

  fclose(fp);

  if (rename(temp_path, sst_meta->disk_path) != 0) {
    perror("Failed to rename file in sst_metadata_write_records");
    return -1;
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

/* SST ITERATORS. */
sst_iterator_t *sst_iterator_create(sst_metadata_record_t *sst) {
  sst_iterator_t *it = calloc(1, sizeof(sst_iterator_t));
  sst_metadata_record_retain(sst);
  it->meta = sst;
  it->fp = NULL;
  it->active = false;
  it->buffer_cap = 1024;
  it->buffer = malloc(it->buffer_cap);
  return it;
}

void sst_iterator_free(sst_iterator_t *it) {
  if (!it) return;
  if (it->meta) sst_metadata_record_release(it->meta);
  if (it->fp) fclose(it->fp);
  if (it->buffer) free(it->buffer);
  free(it);
}

static bool sst_iterator_ensure_open(sst_iterator_t *it) {
  if (!it || !it->meta) return false;

  // Open a file descriptor unique to THIS iterator
  if (it->fp == NULL) {
    it->fp = fopen(it->meta->sstable_filename, "rb");
    if (it->fp == NULL) {
      perror("Failed to open SST file");
      return false;
    }
  }

  // Thread-Safe Lazy Load Index using Double-Checked Locking
  if (it->meta->cached_index == NULL) {
    pthread_mutex_lock(&it->meta->index_lock);

    if (it->meta->cached_index == NULL) {
      it->meta->cached_index = index_load_from_disk(it->meta->sst_index_filename); 
    }

    pthread_mutex_unlock(&it->meta->index_lock);

    if (!it->meta->cached_index) {
      fclose(it->fp);
      it->fp = NULL;
      return false;
    }
  }

  return true;
}

/* Reads the next sst record header.
 * Parses key/len pair, but also returns the raw 21 bytes via out_raw_header.
 * */
static bool sst_read_next_header(FILE *fp, sl_uint128_t *out_key, 
    uint32_t *out_body_len, uint8_t *out_raw_header) {

  if (fread(out_raw_header, 1, 21, fp) != 21) {
    return false;
  }

  memcpy(out_key, out_raw_header, 16);
  memcpy(out_body_len, out_raw_header + 17, 4);
  return true;
}

/* Returns true if the iterator is positioned and has valid data in range.
 * Returns false if the file does not overlap the range (optimization). */
bool sst_iterator_seek(sst_iterator_t *it, sl_uint128_t start, sl_uint128_t end) {
  if (!it || !it->meta) return false;

  sst_metadata_record_t *sst = it->meta;

  /* 
   * If the SSTable is completely out of the query range, do not touch the disk.
   */
  if (key_compare(sst->max_key, start) < 0 || 
      key_compare(sst->min_key, end) > 0) {
    it->active = false;
    return false; 
  }

  if (!sst_iterator_ensure_open(it)) {
    it->active = false;
    return false;
  }

  /* Find closest index block. */
  uint64_t offset = index_lookup(sst->cached_index, start);

  if (fseek(it->fp, offset, SEEK_SET) != 0) {
    it->active = false;
    return false;
  }

  sl_uint128_t key;
  uint32_t body_len;
  uint8_t raw_header[21];

  /* Move from the closest index block position to the closest
   * key of the provided range. */
  while (sst_read_next_header(it->fp, &key, &body_len, raw_header)) {

    /* Went past the key range. */
    if (key_compare(key, end) > 0) {
      it->active = false;
      return false;
    }

    /* Found. */
    if (key_compare(key, start) >= 0) {
      it->end_key = end;
      it->active = true;
      /* The file pointer is now point on the current
       * record body. So i save this header into the struct.
       * The next peek call will use this info.
       */
      it->has_pending_header = true;
      it->pending_key = key;
      it->pending_body_len = body_len;
      memcpy(it->pending_raw_header, raw_header, 21);

      /* Note: current_offset is pointing at the start of this header.
       * We don't update it yet; peek() will update it after consuming.
       * it->current_offset = offset;
       */
      return true;
    }

    /* current key is smaller than the query minimum.
     * just skip to the next record.
     */
    if (fseek(it->fp, body_len, SEEK_CUR) != 0) {
      it->active = false;
      return false;
    }
  }

  /* EOF reached. */
  it->active = false;
  return false; 
}

/*
 * Reads the next record into the internal buffer, returns pointer to it.
 */
static kv_raw_record_t *sst_iterator_peek(sst_iterator_t *it) {
  if (!it || !it->active) return NULL; 
  if (it->buffered_record.valid) return &it->buffered_record;

  sl_uint128_t key;
  uint32_t body_len;
  uint8_t* header_src; 

  if (it->has_pending_header) {
    key = it->pending_key;
    body_len = it->pending_body_len;
    header_src = it->pending_raw_header;
    it->has_pending_header = false;
  }
  else {
    if (!sst_read_next_header(it->fp, &key, &body_len, it->pending_raw_header)) {
      it->active = false;
      return NULL;
    }
    header_src = it->pending_raw_header;
  }

  if (key_compare(key, it->end_key) > 0) {
    it->active = false;
    return NULL;
  }

  size_t total_size = 21 + body_len;

  if (it->buffer_cap < total_size) {
    size_t new_cap = (total_size > it->buffer_cap * 2) ? total_size : it->buffer_cap * 2;
    it->buffer = realloc(it->buffer, new_cap);
    it->buffer_cap = new_cap;
  }
  memcpy(it->buffer, header_src, 21);

  if (fread(it->buffer + 21, 1, body_len, it->fp) != body_len) {
    it->active = false;
    return NULL;
  }

  it->buffered_record.key = key;
  it->buffered_record.raw_data = it->buffer;
  it->buffered_record.total_size = total_size;
  it->buffered_record.valid = true;
  it->current_offset += total_size;

  return &it->buffered_record;
}

// Consumes the current record (invalidates buffer)
static void sst_iterator_advance(sst_iterator_t *it) {
  if (it) it->buffered_record.valid = false;
}

static int compare_sst_its_by_minkey(const void *a, const void *b) {
  sst_iterator_t *it_a = *(sst_iterator_t **)a;
  sst_iterator_t *it_b = *(sst_iterator_t **)b;

  // Access metadata safely
  sl_uint128_t key_a = it_a->meta->min_key;
  sl_uint128_t key_b = it_b->meta->min_key;

  return key_compare(key_a, key_b);
}

sst_k_iterators_t sst_k_iterators_create(sst_metadata_record_t **records, size_t count) {
  sst_k_iterators_t k_its = {0};
  k_its.count = count;
  k_its.iterators = malloc(sizeof(sst_iterator_t*) * count);

  for (size_t i = 0; i < count; i++) {
    k_its.iterators[i] = sst_iterator_create(records[i]);
  }

  // Sort iterators by min_key for the lazy seek optimization
  qsort(k_its.iterators, count, sizeof(sst_iterator_t*), compare_sst_its_by_minkey);
  return k_its;
}

void sst_k_iterators_close(sst_k_iterators_t *it) {
  if (!it) return;
  for (size_t i = 0; i < it->count; i++) {
    sst_iterator_free(it->iterators[i]);
  }
  free(it->iterators);
  it->active = false;
}

void sst_k_iterators_seek(sst_k_iterators_t *it, sl_uint128_t start, sl_uint128_t end) {
  it->seek_start = start;
  it->seek_end = end;
  it->active = true;
  it->next_lazy_idx = 0;

  // Reset all sub-iterators
  for (size_t i = 0; i < it->count; i++) {
    it->iterators[i]->active = false;
    it->iterators[i]->buffered_record.valid = false;
  }
}

// Performs the K-way merge step
kv_raw_record_t *sst_k_iterators_next(sst_k_iterators_t *it) {
  if (!it || !it->active) return NULL;

  sst_iterator_t *winner = NULL;
  kv_raw_record_t *winner_record = NULL;

  /* Check already opened iterators. */
  for (size_t i = 0; i < it->next_lazy_idx; i++) {
    sst_iterator_t *sub = it->iterators[i];
    kv_raw_record_t *rec = sst_iterator_peek(sub);

    if (!rec) continue; // Iterator exhausted or invalid

    if (winner_record == NULL || key_compare(rec->key, winner_record->key) < 0) {
      winner = sub;
      winner_record = rec;
    }
  }

  /* Lazily open pending iterators if they are in range of the current winner. */
  while (it->next_lazy_idx < it->count) {
    sst_iterator_t *pending = it->iterators[it->next_lazy_idx];

    /* If pending SST starts after our current winner, we can stop checking
     * because the iterators are sorted by min_key. */
    if (winner_record != NULL && 
        key_compare(pending->meta->min_key, winner_record->key) > 0) {
      break;
    }

    /* Initialize the pending iterator. */
    sst_iterator_seek(pending, it->seek_start, it->seek_end);
    it->next_lazy_idx++;

    kv_raw_record_t *rec = sst_iterator_peek(pending);
    if (rec) {
      if (winner_record == NULL || key_compare(rec->key, winner_record->key) < 0) {
        winner = pending;
        winner_record = rec;
      }
    }
  }

  if (winner_record) {
    /* Copy the winner to the output (shallow copy is fine, data is in winner's buffer). */
    it->current_record = *winner_record;

    sst_iterator_advance(winner);

    return &it->current_record; 
  }

  it->active = false;
  return NULL;
}

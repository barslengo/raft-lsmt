#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include "lsmt.h"
#include "utils.h"

void test_basic_insert_and_retrieve() {
  printf("Running test_basic_insert_and_retrieve...\n");
  
  // Clean up any old db files in the test folder
  system("rm -rf ./test_db_basic && mkdir -p ./test_db_basic");

  lsmt_t *db = lsmt_init("./test_db_basic");
  assert(db != NULL);

  // Insert 100 keys
  for (uint32_t i = 1; i <= 100; i++) {
    sl_uint128_t key = { .id = i, .timestamp = i };
    uint8_t *content = encode_data(LSMT_TYPE_INT, sizeof(uint32_t), &i);
    int rv = lsmt_insert(db, key, content, 1 + 4 + sizeof(uint32_t));
    assert(rv == 0);
    free(content);
  }

  // Retrieve and verify keys
  lsmt_iterator_t it = lsmt_iterator_create(db);
  sl_uint128_t start = { .id = 1, .timestamp = 0 };
  sl_uint128_t end = { .id = 100, .timestamp = UINT64_MAX };
  lsmt_iterator_seek(&it, start, end);

  for (uint32_t i = 1; i <= 100; i++) {
    kv_raw_record_t rec = lsmt_iterator_next(&it);
    assert(rec.valid);
    assert(rec.key.id == i);

    uint8_t type = rec.raw_data[sizeof(sl_uint128_t)];
    uint32_t len;
    memcpy(&len, rec.raw_data + sizeof(sl_uint128_t) + 1, sizeof(uint32_t));
    assert(type == LSMT_TYPE_INT);
    assert(len == sizeof(uint32_t));

    uint32_t val;
    memcpy(&val, rec.raw_data + sizeof(sl_uint128_t) + 1 + sizeof(uint32_t), sizeof(uint32_t));
    assert(val == i);
  }

  // Check end of iterator
  kv_raw_record_t rec = lsmt_iterator_next(&it);
  assert(!rec.valid);

  lsmt_iterator_close(&it);
  lsmt_free(db);
  
  system("rm -rf ./test_db_basic");
  printf("test_basic_insert_and_retrieve passed!\n");
}

void test_compaction_trigger() {
  printf("Running test_compaction_trigger...\n");

  system("rm -rf ./test_db_compact && mkdir -p ./test_db_compact");

  lsmt_t *db = lsmt_init("./test_db_compact");
  assert(db != NULL);

  // We want to trigger a memtable flush and then compaction.
  // To trigger compaction, we need multiple tiers or files.
  // Let's insert enough records to exceed SIZE_THRESHOLD multiple times.
  // SIZE_THRESHOLD is 2MB. Each record is around 40 bytes.
  // Let's insert 200,000 records.
  printf("Inserting 200,000 records to trigger flushes...\n");
  for (uint32_t i = 1; i <= 200000; i++) {
    sl_uint128_t key = { .id = i, .timestamp = i };
    uint8_t *content = encode_data(LSMT_TYPE_INT, sizeof(uint32_t), &i);
    lsmt_insert(db, key, content, 1 + 4 + sizeof(uint32_t));
    free(content);
  }

  printf("Flushing remaining memtable...\n");
  lsmt_flush(db);

  // Since we flushed everything, let's verify that we can still read all keys correctly
  printf("Verifying keys after flushes...\n");
  lsmt_iterator_t it = lsmt_iterator_create(db);
  sl_uint128_t start = { .id = 1, .timestamp = 0 };
  sl_uint128_t end = { .id = 200000, .timestamp = UINT64_MAX };
  lsmt_iterator_seek(&it, start, end);

  for (uint32_t i = 1; i <= 200000; i++) {
    kv_raw_record_t rec = lsmt_iterator_next(&it);
    assert(rec.valid);
    assert(rec.key.id == i);
  }
  lsmt_iterator_close(&it);

  lsmt_free(db);
  system("rm -rf ./test_db_compact");
  printf("test_compaction_trigger passed!\n");
}

int main() {
  test_basic_insert_and_retrieve();
  test_compaction_trigger();
  printf("All tests completed successfully!\n");
  return 0;
}

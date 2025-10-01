#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include "lsmt.h"
#include "utils.h"

void print_content(lsmt_t *db, sl_uint128_t key) {
  uint8_t *res = lsmt_get(db, key);

  if (res == NULL) { 
    printf("[NOT FOUND] key=%lu%lu\n", key.id, key.timestamp);
    fflush(stdout);
    return;
  }

  /* Decoding the stored data.
   * Here i'm assuming that every data stored is a uint32_t type,
   * i should instead do the casting accordingly to the data type.
   */
  uint8_t type = *res;
  uint32_t content_size = *((uint32_t *)(res+1));
  uint32_t content = *(uint32_t *)(res+5);

  printf("[FOUND] key=%lu%lu, type=%d, length=%d, content=%d\n", key.id, key.timestamp, type, content_size, content);
  fflush(stdout);
  free(res);
}

void test(size_t n_entries) {
  printf("Test running for %ld entries ... \n", n_entries);
  lsmt_t *db = lsmt_init();

  sl_uint128_t key1 = { 0 };
  sl_uint128_t last_keys[50];
  size_t j = 0;

  for (size_t i = 0; i < n_entries; i++) {
    uint32_t key_id = (rand() % (n_entries << 1)) + 1;
    uint8_t *content = encode_data(LSMT_TYPE_INT, sizeof(uint32_t), &key_id);

    sl_uint128_t key = {
      .id = key_id,
      .timestamp = get_unix_epoch()
    };

    if (lsmt_insert(db, key, content, 1 + 4 + sizeof(uint32_t)) == 0) {
      if (j == 3) key1 = key;
      last_keys[j % 50] = key;
      j++;
    }

    free(content);
  }


  sl_uint128_t key2 = last_keys[(j-1)%50];
  lsmt_flush(db);
  printf("DB populated ... \n");
  fflush(stdout);

  print_content(db, key1);
  print_content(db, key2);

  //printf("levels: %ld\n", db->memtable->levels);
  //size_t total_size = db->memtable->size + sizeof(sl_t);
  //printf("allocated memory: %ld bytes (~ %.3f KB, %.3f MB)\n", total_size, (double)total_size/(1 << 10), (double)total_size/(1 << 20)); 

  //lsmt_free(db);
} 
int main(int argc, char *argv[]) {
  srand(time(NULL));

  size_t entries_to_generate = rand();
  if (argc > 1) {
    char *endptr;
    entries_to_generate = (size_t)strtoull(argv[1], &endptr, 10); 
  }

  test(entries_to_generate);
  return 0;
}

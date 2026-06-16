#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include "lsmt.h"
#include "utils.h"

void test(size_t n_entries) {
  printf("Test running for %ld entries ... \n", n_entries);
  lsmt_t *db = lsmt_init("./");

  for (size_t i = 0; i < n_entries; i++) {
    uint32_t key_id = i;
    sl_uint128_t key = {
      .id = key_id,
      .timestamp = get_unix_epoch()
    };
    uint8_t *content = encode_data(LSMT_TYPE_INT, sizeof(sl_uint128_t), &key);
    lsmt_insert(db, key, content, 1 + 4 + sizeof(sl_uint128_t));
    free(content);
  }

  lsmt_flush(db);
  printf("DB populated ... \n");
  fflush(stdout);

  //printf("levels: %ld\n", db->memtable->levels);
  //size_t total_size = db->memtable->size + sizeof(sl_t);
  //printf("allocated memory: %ld bytes (~ %.3f KB, %.3f MB)\n", total_size, (double)total_size/(1 << 10), (double)total_size/(1 << 20)); 

  lsmt_free(db);
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

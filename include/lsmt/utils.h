#ifndef UTILS_H
#define UTILS_H

#include <stdint.h>

typedef struct sl_uint128 {
  uint64_t id;
  uint64_t timestamp;
} sl_uint128_t;


uint64_t get_unix_epoch();

#endif

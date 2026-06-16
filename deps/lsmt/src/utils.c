#include <time.h>
#include "utils.h"

uint64_t get_unix_epoch() {
  struct timespec now;
  timespec_get(&now, TIME_UTC);

  return ((uint64_t) now.tv_sec) * 1000 + ((uint64_t) now.tv_nsec) / 1000000;
}

uint64_t get_monotonic_time_ms() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}




#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/resource.h>
#include <raft/raft.h>
#include <raft/raft/uv.h>

#include <lsmt/lsmt.h>

#define APPLY_RATE 100 /* Store new statistic entry every 100 ms. */
#define GLOBAL_MAX_QUERIES 8 // Limite massimo di query simultanee nel server
#define MAX_LATENCY_SAMPLES 1000 /* Max samples for latency distribution */
#define STORAGE_EVENT_BUFFER_SIZE 1000 /* Buffer size for storage events */
#define Log(SERVER_ID, FORMAT) printf("%d: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
  printf("%d: " FORMAT "\n", SERVER_ID, __VA_ARGS__)


#define BATCH_SIZE 4096 //1024 //128
#define CONTENT_MAX_SIZE 255
#define QUERY_BYTES_LIMIT (512 * 1024) //512 KB 
#define QUERY_REQ_SIZE (sizeof(uint64_t) + 2 * sizeof(sl_uint128_t)) //40 bytes.
#define QUERY_BUDGET_NS (30 * 1000 * 1000)  // 30ms 

static char ACK_BUFFER[BATCH_SIZE];
const uint8_t CONTENT_HEADER_SIZE = sizeof(sl_uint128_t) + sizeof(uint8_t); 
const uint32_t INSERT_CMD_SIZE = CONTENT_HEADER_SIZE + sizeof(uint8_t)*CONTENT_MAX_SIZE; 

typedef struct __attribute__((packed)) {
    uint64_t timestamp;
    uint32_t batch_size;
    uint32_t bytes;
    uint64_t t_accumulate;
    uint64_t t_consensus;
    uint64_t t_total;
} batch_log_t;

/* Circular buffer for latency samples */
typedef struct {
    uint64_t samples[MAX_LATENCY_SAMPLES];
    size_t head;
    size_t count;
} latency_buffer_t;

/* Storage system event types */
typedef enum {
    STORAGE_EVENT_COMPACTION,
    STORAGE_EVENT_MEMTABLE_FLUSH
} storage_event_type_t;

/* Buffered storage event */
typedef struct {
    storage_event_type_t type;
    uint64_t timestamp;
    uint64_t duration_ms;
    union {
        struct {
            uint32_t quantity_merged_tables;
            uint64_t input_bytes;
            uint64_t output_bytes;
            uint8_t level;
        } compaction;
        struct {
            uint64_t bytes_flushed;
        } memtable_flush;
    } data;
} storage_event_t;

typedef struct query_response {
  bool limit_reached;
  uint32_t records_count;
  uint64_t req_id;
  uint64_t total_msg_size;
  uint64_t records_bytes;
  sl_uint128_t min_key;
  sl_uint128_t max_key;
} query_response_t;

/* QUERY RESPONSE MSG SIZE. */
static query_response_t __query_msg;
static const uint32_t Q_RESP_HEADER_SIZE = sizeof(__query_msg.total_msg_size) + 
                                    sizeof(__query_msg.req_id) + 
                                    sizeof(__query_msg.limit_reached) +
                                    sizeof(__query_msg.min_key) + 
                                    sizeof(__query_msg.max_key) +
                                    sizeof(__query_msg.records_bytes) + 
                                    sizeof(__query_msg.records_count); 

/* ========== CLUST CONFIG SECTION START ========== */
typedef struct {
  uint32_t id;
  char raft_address[64];
  int client_port;
} node_config_t;

typedef struct {
  node_config_t *nodes;
  int count;
} cluster_config_t;

int load_cluster_config(const char *filename, cluster_config_t *out_conf) {
  FILE *f = fopen(filename, "r");
  if (!f) return -1;

  // Count lines to allocate memory
  int lines = 0;
  char ch;
  while(!feof(f)) {
    ch = fgetc(f);
    if(ch == '\n') lines++;
  }
  // Handle case where last line has no newline
  lines++; 
  rewind(f);

  out_conf->nodes = calloc(lines, sizeof(node_config_t));
  out_conf->count = 0;

  // 2. Parse lines
  char line[256];
  int i = 0;
  while (fgets(line, sizeof(line), f)) {
    // Skip empty lines or comments
    if (strlen(line) < 5 || line[0] == '#') continue;

    node_config_t *node = &out_conf->nodes[i];

    // Format: ID RAFT_ADDR CLIENT_PORT
    int n = sscanf(line, "%u %63s %d", 
        &node->id, 
        node->raft_address, 
        &node->client_port);

    if (n == 3) {
      i++;
    }
  }

  out_conf->count = i;
  fclose(f);
  return 0;
}

/* ========== CLUST CONFIG SECTION END ========== */


typedef struct insert_cmd {
  uint32_t msg_size;
  sl_uint128_t record_key;
  uint8_t record_value[CONTENT_MAX_SIZE];
} insert_cmd_t;

lsmt_t *db;

struct Server;

struct Fsm
{
  struct Server *server;
  insert_cmd_t insert_cmd;
};


/* Context specific to INSERT clients */
typedef struct {
  void *batch_buffer;          /* The buffer for Raft batching */
  uint32_t batch_offset;       /* Current write position in batch_buffer */
  uint32_t buffer_capacity;     /* Total size of batch_buffer */
  uint32_t batched_req_count;  /* Number of items currently in batch */
  uint64_t batch_start_ts;     /* Timestamp (ms) when first item arrived */
} client_insert_ctx_t;

/* Context specific to QUERY clients */
typedef struct {
  uint64_t active_query_start_ts; /* Timestamp (ms) of current processing query */
  uint64_t total_queries;         /* Total queries processed by this client */
  bool is_paused;
} client_query_ctx_t;

typedef enum {
  CLIENT_TYPE_INSERT,
  CLIENT_TYPE_QUERY
} client_type_t;

typedef struct client_t {
  client_type_t type;
  uv_tcp_t handle;
  struct Server *server;

  // A dynamic buffer to handle the incoming TCP stream
  char *buffer;
  size_t buffer_len;
  size_t buffer_cap;

  bool closing;
  int ref_count;

  union {
    client_insert_ctx_t insert;
    client_query_ctx_t query;
  } ctx; 


  /* Linked List. */
  struct client_t *prev;
  struct client_t *next;
} client_t;

typedef struct {
  uv_write_t req;
  uv_buf_t buf;
  client_t *client;
} ack_write_t;

typedef struct {
  struct Server *server;
  client_t *client;
  int req_count;

  uint32_t batch_size_bytes;
  uint64_t batch_creation_ts; //milliseconds
  uint64_t raft_apply_ts; //milliseconds
} apply_ctx_t;

/********************************************************************
 *
 * struct holding a single raft server instance and all its
 * dependencies.
 *
 ********************************************************************/

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

struct Server
{
  int last_state;                // To track state changes (Leader <-> Follower)
  lsmt_t *db;
  uv_tcp_t tcp_write_handle;
  uv_tcp_t tcp_read_handle;
  void *data;                         /* User data context. */
  struct uv_loop_s *loop;             /* UV loop. */
  struct uv_timer_s timer;            /* To periodically apply a new entry. */
  const char *dir;                    /* Data dir of UV I/O backend. */
  struct raft_uv_transport transport; /* UV I/O backend transport. */
  struct raft_io io;                  /* UV I/O backend. */
  struct raft_fsm fsm;                /* Sample application FSM. */
  unsigned id;                        /* Raft instance ID. */
  char address[64];                   /* Raft instance address. */
  struct raft raft;                   /* Raft instance. */
  struct raft_transfer transfer;      /* Transfer leadership request. */
  ServerCloseCb close_cb;             /* Optional close callback. */

  struct client_t *insert_clients_head;
  struct client_t *query_clients_head;
  int active_queries_count;

  
  /* Read metrics */
  struct {
    uint64_t total_queries;
    uint64_t total_bytes_read;
    uint64_t prev_queries;
    uint64_t prev_bytes_read;
  } read_stats;
  
  /* Latency distribution tracking */
  latency_buffer_t latency_buffer;
  
  /* Storage events */
  FILE *storage_events_file;
  storage_event_t storage_events_buffer[STORAGE_EVENT_BUFFER_SIZE];
  size_t storage_events_count;
  uv_mutex_t storage_events_mutex;
  
  struct {
    FILE *f;
    uint64_t total_requests;
    uint64_t total_bytes;

    uint64_t prev_requests;
    uint64_t prev_bytes;

    //uint64_t total_received; /* Total requests received from tcp connections. */
    uint64_t last_run_time; 
    uint64_t period_latency_sum; /* Sum of ms taken by all batches in this tick */
    uint64_t period_batches_count; /* How many batches finished in this tick */
  } stats;

};

/* Initialize latency buffer */
static void latency_buffer_init(latency_buffer_t *buf) {
  buf->head = 0;
  buf->count = 0;
}

/* Add a latency sample to the buffer */
static void latency_buffer_add(latency_buffer_t *buf, uint64_t latency_ms) {
  buf->samples[buf->head] = latency_ms;
  buf->head = (buf->head + 1) % MAX_LATENCY_SAMPLES;
  if (buf->count < MAX_LATENCY_SAMPLES) {
    buf->count++;
  }
}

/* Calculate percentile from sorted samples (caller must ensure samples are sorted) */
static double calculate_percentile(uint64_t *sorted_samples, size_t count, double percentile) {
  if (count == 0) return 0.0;
  if (percentile >= 100.0) return (double)sorted_samples[count - 1];
  if (percentile <= 0.0) return (double)sorted_samples[0];
  
  double index = percentile / 100.0 * (count - 1);
  size_t lower = (size_t)index;
  size_t upper = lower + 1;
  if (upper >= count) upper = lower;
  
  double weight = index - lower;
  return (1.0 - weight) * sorted_samples[lower] + weight * sorted_samples[upper];
}

/* Callback for compaction events from lsmt */
static void on_compaction_event(void *user_data, uint64_t ts, uint64_t duration_ms,
    uint32_t quantity_merged_tables, uint64_t input_bytes, uint64_t output_bytes, uint8_t level) {
  struct Server *s = (struct Server *)user_data;

  uv_mutex_lock(&s->storage_events_mutex);

  if (s && s->storage_events_count < STORAGE_EVENT_BUFFER_SIZE) {
    storage_event_t *evt = &s->storage_events_buffer[s->storage_events_count++];

    evt->type = STORAGE_EVENT_COMPACTION;
    evt->timestamp = ts;
    evt->duration_ms = duration_ms;
    evt->data.compaction.quantity_merged_tables = quantity_merged_tables;
    evt->data.compaction.input_bytes = input_bytes;
    evt->data.compaction.output_bytes = output_bytes;
    evt->data.compaction.level = level;
  }
  uv_mutex_unlock(&s->storage_events_mutex);
}

/* Callback for memtable flush events from lsmt */
static void on_memtable_flush_event(void *user_data, uint64_t ts, uint64_t duration_ms,
    uint64_t bytes_flushed) {
  struct Server *s = (struct Server *)user_data;
  if (!s) return;

  uv_mutex_lock(&s->storage_events_mutex);

  if (s && s->storage_events_count < STORAGE_EVENT_BUFFER_SIZE) {
    storage_event_t *evt = &s->storage_events_buffer[s->storage_events_count++];

    evt->type = STORAGE_EVENT_MEMTABLE_FLUSH;
    evt->timestamp = ts; 
    evt->duration_ms = duration_ms;
    evt->data.memtable_flush.bytes_flushed = bytes_flushed;
  }

  uv_mutex_unlock(&s->storage_events_mutex);
}

/* Flush storage events buffer to CSV file */
static void storage_events_flush(struct Server *s) {
  if (!s->storage_events_file) return;

  uv_mutex_lock(&s->storage_events_mutex);
  if (s->storage_events_count == 0) {
    uv_mutex_unlock(&s->storage_events_mutex);
    return;
  }

  for (size_t i = 0; i < s->storage_events_count; i++) {
    storage_event_t *evt = &s->storage_events_buffer[i];
    if (evt->type == STORAGE_EVENT_COMPACTION) {
      fprintf(s->storage_events_file, "compaction,%lu,%lu,%u,%lu,%lu,%u,0\n",
          evt->timestamp,
          evt->duration_ms,
          evt->data.compaction.quantity_merged_tables,
          evt->data.compaction.input_bytes,
          evt->data.compaction.output_bytes,
          evt->data.compaction.level);
    } else if (evt->type == STORAGE_EVENT_MEMTABLE_FLUSH) {
      fprintf(s->storage_events_file, "memtable_flush,%lu,%lu,0,0,0,0,%lu\n",
          evt->timestamp,
          evt->duration_ms,
          evt->data.memtable_flush.bytes_flushed);
    }
  }
  fflush(s->storage_events_file);
  s->storage_events_count = 0;

  uv_mutex_unlock(&s->storage_events_mutex);
}

typedef struct {
  uv_work_t work_req;
  client_t *client;

  // Request data
  uint64_t req_id;
  sl_uint128_t start_key;
  sl_uint128_t end_key;

  // Response data
  uint8_t *response_msg;
  uint64_t total_msg_size;

  // Profiling metrics
  uint64_t t_start_ns;
  uint64_t t_iter_us;
  uint64_t records_count;
} query_task_t;

void client_retain(client_t *c) {
  c->ref_count++;
}

void client_release(client_t *c) {
  c->ref_count--;
  if (c->ref_count == 0) {
    if (c->buffer) free(c->buffer);

    if (c->type == CLIENT_TYPE_INSERT) {
      if (c->ctx.insert.batch_buffer) free(c->ctx.insert.batch_buffer); 
    }

    free(c);
  }
}

/* Add to head of list */
static void add_client(struct Server *s, client_t *c, client_type_t type) {
  c->type = type;  
  client_t **head_ptr = (type == CLIENT_TYPE_INSERT) 
    ? &s->insert_clients_head 
    : &s->query_clients_head;

  c->next = *head_ptr;
  c->prev = NULL;
  if (*head_ptr) {
    (*head_ptr)->prev = c;
  }
  *head_ptr = c;
}

/* Remove from list */
static void remove_client(struct Server *s, client_t *c) {
  client_t **head_ptr = (c->type == CLIENT_TYPE_INSERT) 
    ? &s->insert_clients_head 
    : &s->query_clients_head;


  if (c->prev) {
    c->prev->next = c->next;
  }
  else if (*head_ptr == c) {
    *head_ptr = c->next;
  }

  if (c->next) {
    c->next->prev = c->prev;
  }

  c->next = NULL;
  c->prev = NULL;
}

static void set_query_response_header(uint8_t *out, query_response_t response) {
  uint64_t offset = 0;
  // Header Serialization
  memcpy(out, &response.total_msg_size, sizeof(response.total_msg_size));
  offset += sizeof(response.total_msg_size);

  memcpy(out + offset, &response.req_id, sizeof(response.req_id));
  offset += sizeof(response.req_id);

  memcpy(out + offset, &response.limit_reached, sizeof(response.limit_reached));
  offset += sizeof(response.limit_reached);

  memcpy(out + offset, &response.min_key, sizeof(response.min_key));
  offset += sizeof(response.min_key);
  memcpy(out + offset, &response.max_key, sizeof(response.max_key));
  offset += sizeof(response.max_key);

  /* Body header. */
  memcpy(out + offset, &response.records_bytes, sizeof(response.records_bytes));
  offset += sizeof(response.records_bytes);
  memcpy(out + offset, &response.records_count, sizeof(response.records_count));
  offset += sizeof(response.records_count);

  if (offset != Q_RESP_HEADER_SIZE) exit(1); 
  //printf("------ %lu ------\n", offset);
}

/* 
 *  Query request format:
 * [REQ_ID (8 Bytes) | START_KEY (16 Bytes) | END_KEY (16 bytes)]
 * For the response format consult the 'serialize_response' function.
 *
 * Query Response format: 
 * Header: [TOTAL_MSG_SIZE (8 Bytes) | REQ_ID (8 Bytes) | LIMIT (1 Byte) | MIN_KEY (16 bytes) | MAX_KEY (16 bytes)]
 * Body: [PAYLOAD_SIZE (8 Bytes) | RECORDS_COUNT (4 Bytes) | RECORDS]
 *
 *
 * For each query request:
 * Lookup into the lsmt database for the key range provided, the lookup
 * is bounded to a fixed amount of bytes. The response returns the fetched
 * records, a flag indicating whether the byte limit has been reached
 * and the last fetched key.
 *
 * This function has a time budget in order to set an upper time limit
 * for a each event client-query, thus avoiding blocking the whole system
 * if the are too much requests buffered.
 */
static void background_query_cb(uv_work_t *req) {
  query_task_t *task = (query_task_t *)req->data;

  uint64_t t_iter_start = uv_hrtime();
  uint64_t deadline = t_iter_start + QUERY_BUDGET_NS;

  lsmt_iterator_t it = lsmt_iterator_create(db);

  size_t max_buffer_cap = Q_RESP_HEADER_SIZE + QUERY_BYTES_LIMIT + 4096;
  task->response_msg = malloc(max_buffer_cap);

  if (!task->response_msg) {
    lsmt_iterator_close(&it);
    fprintf(stderr, "OOM allocating response buffer\n");
    return;
  }

  uint64_t msg_offset = Q_RESP_HEADER_SIZE;
  uint32_t records_count = 0;
  size_t records_bytes = 0;
  bool limit_reached = false;

  sl_uint128_t min_key = { .id = UINT64_MAX, .timestamp = UINT64_MAX };
  sl_uint128_t max_key = { .id = 1, .timestamp = 0 };

  /*
   * If the request has been idle for more than 8 seconds, then skip the search
   * and respond back.
   */
  if (t_iter_start - task->t_start_ns > 8000000000ULL) {
    limit_reached = true;
    goto finalize_response;
  }

  lsmt_iterator_seek(&it, task->start_key, task->end_key);

  while (it.active) {
    if (records_count > 0 && uv_hrtime() > deadline) {
      limit_reached = true;
      break;
    }

    kv_raw_record_t record = lsmt_iterator_next(&it);
    if (!it.active) break;

    if (records_bytes + record.total_size > QUERY_BYTES_LIMIT) {
      limit_reached = true;
      if (records_count > 0) break;
    }

    if (records_count == 0) {
      min_key = record.key;
    }
    max_key = record.key;

    memcpy(task->response_msg + msg_offset, record.raw_data, record.total_size);
    msg_offset += record.total_size;
    records_bytes += record.total_size;
    records_count++;
  }

finalize_response:
  lsmt_iterator_close(&it);

  task->t_iter_us = (uv_hrtime() - t_iter_start) / 1000;
  task->records_count = records_count;

  if (records_count == 0) {
    min_key.id = UINT64_MAX; min_key.timestamp = UINT64_MAX;
    max_key.id = 1; max_key.timestamp = 0;
    records_bytes = 0;
  }

  query_response_t msg_header = {0};
  msg_header.total_msg_size = Q_RESP_HEADER_SIZE + records_bytes; 
  msg_header.req_id = task->req_id;
  msg_header.limit_reached = limit_reached;
  msg_header.min_key = min_key;
  msg_header.max_key = max_key;
  msg_header.records_bytes = records_bytes;
  msg_header.records_count = records_count;

  set_query_response_header(task->response_msg, msg_header);
  task->total_msg_size = msg_header.total_msg_size;
}

static void on_client_close(uv_handle_t *handle);

static void close_and_free_client(client_t *client) {
  if (client->closing) return;
  client->closing = true;

  remove_client(client->server, client);
  uv_close((uv_handle_t *)&client->handle, on_client_close);
}

static void on_query_resp_complete(uv_write_t *req, int status) {
  ack_write_t *wr = (ack_write_t *)req;
  //printf("Write sent\n");
  if (status) {
    close_and_free_client(wr->client);
  }

  if (wr->buf.base) {
    free(wr->buf.base);
  }

  client_release(wr->client);
  free(wr);
}

static int send_response(client_t *c, uint8_t *data, uint64_t payload_size) {
  struct Server *s = c->server;
  
  /* Track read metrics */
  s->read_stats.total_queries++;
  s->read_stats.total_bytes_read += payload_size;
  
  ack_write_t *wr = malloc(sizeof(ack_write_t));
  if (!wr) {
    free(data); 
    exit(1);
  }

  wr->req.data = wr;
  wr->client = c;

  // Retain client so it isn't freed while write is pending
  client_retain(c);

  wr->buf = uv_buf_init((char*)data, (unsigned int)payload_size);

  //printf("Writing back %u bytes.\n", payload_size);
  int r = uv_write(&wr->req, (uv_stream_t*)&c->handle, &wr->buf, 1, on_query_resp_complete);

  if (r != 0) {
    // Write failed immediately (didn't queue).
    free(data); 
    client_release(c);
    free(wr);
    close_and_free_client(c);
    return -1;
  }
  return 0;
}

static void after_query_cb(uv_work_t *req, int status);

static void dispatch_queries(client_t *client) {
  struct Server *s = client->server;
  size_t offset = 0;

  // Process all fully buffered queries in the pipeline
  while (client->buffer_len - offset >= QUERY_REQ_SIZE) {

    if (s->active_queries_count >= GLOBAL_MAX_QUERIES) {
      if (!client->ctx.query.is_paused) {
        uv_read_stop((uv_stream_t *)&client->handle);
        client->ctx.query.is_paused = true;
        // printf("Server saturo: Lettura TCP dal client in pausa.\n");
      }
      break;
    }

    uint64_t req_id;
    sl_uint128_t start_key;
    sl_uint128_t end_key;

    memcpy(&req_id, client->buffer + offset, sizeof(uint64_t)); 
    offset += sizeof(uint64_t);
    memcpy(&start_key, client->buffer + offset, sizeof(sl_uint128_t)); 
    offset += sizeof(sl_uint128_t);
    memcpy(&end_key, client->buffer + offset, sizeof(sl_uint128_t)); 
    offset += sizeof(sl_uint128_t);

    query_task_t *task = calloc(1, sizeof(query_task_t));
    task->work_req.data = task;
    task->client = client;

    // Retain client so it isn't destroyed while the thread is running
    client_retain(client);

    task->req_id = req_id;
    task->start_key = start_key;
    task->end_key = end_key;
    task->t_start_ns = uv_hrtime();

    s->active_queries_count++;

    if (uv_queue_work(client->server->loop, &task->work_req, background_query_cb, after_query_cb) != 0) {
      s->active_queries_count--;
      client_release(client);
      free(task);
    }
    //uv_queue_work(client->server->loop, &task->work_req, background_query_cb, after_query_cb);
  }

  // Shift remaining partial bytes back to the start of the buffer
  if (offset > 0) {
    size_t remaining = client->buffer_len - offset;
    if (remaining > 0) {
      memmove(client->buffer, client->buffer + offset, remaining);
    }
    client->buffer_len = remaining;
  }
}

static int FsmApply(struct raft_fsm *fsm,
    const struct raft_buffer *buf,
    void **result)
{
  static size_t count = 0;
  struct Fsm *f = fsm->data;
  struct Server *s = f->server;

  /* We treat the buffer as a stream of concatenated commands. 
   * We loop until we have consumed the entire buffer length. */
  size_t offset = 0;
  uint64_t requests_count = 0;

  /* Iterate until i don't have enought space even
   * for reading the msg size (4 bytes). */
  while (offset + sizeof(uint32_t) <= buf->len) {
    uint32_t msg_size;

    memcpy(&msg_size, (const uint8_t *)buf->base + offset,
        sizeof(uint32_t)); 

    /* I've reached the 0-filled padding added by the batch_flush. */
    if (msg_size == 0) {
      break;
    }

    /* Validation: Ensure the declared size fits in the remaining buffer */
    if (msg_size + offset > buf->len) {
      printf("FAILED\n");
      return RAFT_MALFORMED;
    }

    /* Validation: Ensure the message is at least as big as the header */
    /* Header = Size(4) + Key(16) */
    if (msg_size < sizeof(uint32_t) + sizeof(sl_uint128_t)) {
      printf("FAILED_2\n");
      return RAFT_MALFORMED;
    }

    /* Calculate record value size */
    const uint32_t record_value_size = msg_size - sizeof(uint32_t) - sizeof(sl_uint128_t);

    if (record_value_size > CONTENT_MAX_SIZE) {
      printf("FAILED_3\n");
      return RAFT_MALFORMED;
    }

    /* Deserialize into the persistent struct in fsm->data */
    const uint8_t *data_ptr = (const uint8_t *)buf->base + offset;

    /* Skip the size (4 bytes), copy the key (16 bytes) */
    memcpy(&f->insert_cmd.record_key,
        data_ptr + sizeof(uint32_t), 
        sizeof(sl_uint128_t));

    /* Copy the value */
    memcpy(f->insert_cmd.record_value,
        data_ptr + sizeof(uint32_t) + sizeof(sl_uint128_t), 
        record_value_size);

    /*
       uint32_t value_size = *((uint32_t *)(f->insert_cmd.record_value + sizeof(uint8_t)));
       printf("insert: key=%lu %lu, record_length=%u, value_type=%u, value_size=%u, value_content: \n[",
       f->insert_cmd.record_key.id,
       f->insert_cmd.record_key.timestamp,
       record_value_size,
       f->insert_cmd.record_value[0],
       value_size);

       uint32_t value_offset = record_value_size - value_size;
       for (uint32_t i = record_value_size-1; i >= value_offset; i--) {
       printf(" %02x", f->insert_cmd.record_value[i]);
       }
       printf("]\n\n");
       fflush(stdout);
       */

    int e = lsmt_insert(db, f->insert_cmd.record_key,
        f->insert_cmd.record_value, 
        record_value_size);

    if (count % 100000 == 0) {
      printf("%lu\n", count);
    }
    count++;

    if (e != 0) {
      Logf(s->id, "Warning: lsmt_insert failed with %d.", e);
      //return RAFT_INVALID;
    }
    /* Advance the offset to the next message in the batch */
    offset += msg_size;

    requests_count++;
  }

  s->stats.total_requests += requests_count;
  s->stats.total_bytes += buf->len;
  return 0;
}

static int FsmSnapshot(struct raft_fsm *fsm,
    struct raft_buffer *bufs[],
    unsigned *n_bufs)
{
  struct Fsm *f = fsm->data;
  *n_bufs = 1;
  *bufs = raft_malloc(sizeof **bufs);
  if (*bufs == NULL) {
    return RAFT_NOMEM;
  }
  (*bufs)[0].len = sizeof(insert_cmd_t); //sizeof(uint64_t);
  (*bufs)[0].base = raft_malloc((*bufs)[0].len);
  if ((*bufs)[0].base == NULL) {
    raft_free(*bufs);
    return RAFT_NOMEM;
  }
  //*(uint64_t *)(*bufs)[0].base = f->count;
  return 0;
}

static int FsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
  struct Fsm *f = fsm->data;
  if (buf->len != sizeof(insert_cmd_t)) {
    return RAFT_MALFORMED;
  }
  //f->count = *(uint64_t *)buf->base;
  //raft_free(buf->base);
  return 0;
}

static int FsmInit(struct raft_fsm *fsm, struct Server *s)
{
  struct Fsm *f = raft_malloc(sizeof *f);
  if (f == NULL) {
    return RAFT_NOMEM;
  }
  memset(f, 0, sizeof(*f));
  f->server = s;
  fsm->version = 2;
  fsm->data = f;
  fsm->apply = FsmApply;
  fsm->snapshot = FsmSnapshot;
  fsm->snapshot_finalize = NULL;
  fsm->restore = FsmRestore;
  return 0;
}

static void FsmClose(struct raft_fsm *f)
{
  if (f->data != NULL) {
    raft_free(f->data);
  }
}


static void serverRaftCloseCb(struct raft *raft)
{
  struct Server *s = raft->data;
  raft_uv_close(&s->io);
  raft_uv_tcp_close(&s->transport);
  FsmClose(&s->fsm);
  if (s->close_cb != NULL) {
    s->close_cb(s);
  }
}

static void serverTransferCb(struct raft_transfer *req)
{
  struct Server *s = req->data;
  raft_id id;
  const char *address;
  raft_leader(&s->raft, &id, &address);
  raft_close(&s->raft, serverRaftCloseCb);
}

static void on_redirect_write_complete(uv_write_t *req, int status) {
  ack_write_t *wr = (ack_write_t *)req;

  close_and_free_client(wr->client);
  client_release(wr->client);
  free(wr->buf.base);
  free(wr);
}

static void redirect_to_leader_and_close(client_t *client) {
  struct Server *s = client->server;
  raft_id leader_id;
  const char *leader_addr;

  raft_leader(&s->raft, &leader_id, &leader_addr);

  char *msg = malloc(256);
  if (!msg) {
    close_and_free_client(client);
    return;
  }

  int len = snprintf(msg, 256, "REDIRECT %llu %s\n", 
      (unsigned long long)leader_id, 
      leader_addr ? leader_addr : "UNKNOWN");

  printf("%s\n", msg);

  ack_write_t *wr = malloc(sizeof(ack_write_t));
  if (!wr) {
    free(msg);
    close_and_free_client(client);
    return;
  }

  uv_read_stop((uv_stream_t*)&client->handle);

  wr->req.data = wr;
  wr->client = client;
  client_retain(client);
  wr->buf = uv_buf_init(msg, len);

  int r = uv_write(&wr->req, (uv_stream_t*)&client->handle, &wr->buf, 1, on_redirect_write_complete);
  if (r != 0) {
    free(msg);
    client_release(client);
    free(wr);
    close_and_free_client(client);
  }
}


/* Final callback in the shutdown sequence, invoked after the timer handle has
 * been closed. */
static void serverTimerCloseCb(struct uv_handle_s *handle)
{
  struct Server *s = handle->data;
  if (s->raft.data != NULL) {
    if (s->raft.state == RAFT_LEADER) {
      int rv;
      rv = raft_transfer(&s->raft, &s->transfer, 0, serverTransferCb);
      if (rv == 0) {
        return;
      }
    }
    raft_close(&s->raft, serverRaftCloseCb);
  }
}

static void serverApplyCb(struct raft_apply *req, int status, void *result);

static bool process_insert_buffer(client_t *client);


// Helper function to safely close and free a client's resources


// Callback that fires after a client's handle is fully closed
static void on_client_close(uv_handle_t *handle) {
  client_t *client = (client_t *)handle->data;
  client_release(client);
}



static void on_ack_write_complete(uv_write_t *req, int status) {
  ack_write_t *wr = (ack_write_t *)req;
  if (status) {
    close_and_free_client(wr->client);
  }

  client_release(wr->client);
  free(wr);
}

static void batch_buffer_flush(struct Server *s, client_t *c) {
  client_insert_ctx_t *ins_ctx = &c->ctx.insert;
  if (ins_ctx->batched_req_count <= 0) {
    return;
  }

  struct raft_buffer raft_buf;

  uint32_t aligned_msg_size = (ins_ctx->batch_offset + 7) & ~0x07;
  raft_buf.len = aligned_msg_size;
  raft_buf.base = raft_malloc(raft_buf.len);

  if (!raft_buf.base) {
    fprintf(stderr, "Critical: Out of memory in flush_batch\n");
    exit(1);
  }

  /* Copy from the reusable batch buffer to the Raft entry buffer */
  memset(raft_buf.base, 0, raft_buf.len);
  memcpy(raft_buf.base, ins_ctx->batch_buffer, ins_ctx->batch_offset);

  struct raft_apply *req = raft_malloc(sizeof(*req));
  if (!req) {
    fprintf(stderr, "Critical: Out of memory for req in flush_batch\n");
    exit(1);
  }

  apply_ctx_t *ctx = malloc(sizeof(apply_ctx_t));
  ctx->server = s;
  ctx->client = c;
  client_retain(c);
  ctx->req_count = ins_ctx->batched_req_count;

  ctx->batch_size_bytes = ins_ctx->batch_offset;
  ctx->batch_creation_ts = ins_ctx->batch_start_ts;
  ctx->raft_apply_ts = uv_now(s->loop);

  req->data = ctx;

  /* Apply the batch as ONE log entry */
  int rv = raft_apply(&s->raft, req, &raft_buf, 1, serverApplyCb);
  /*
     printf("[BATCH saved] buffer (aligned) size=%lu, payload_len=%u\n",
     raft_buf.len, s->batch_offset);
     fflush(stdout);
     */


  if (rv != 0) {
    Logf(s->id, "raft_apply() failed: %s", raft_errmsg(&s->raft));
    client_release(c);
    free(ctx);
    raft_free(raft_buf.base);
    raft_free(req);
    close_and_free_client(c);
  }

  /* Reset Batch State */
  ins_ctx->batch_offset = 0;
  ins_ctx->batched_req_count = 0;
  memset(ins_ctx->batch_buffer, 0, ins_ctx->buffer_capacity); // Optional: clear buffer for debug
}

// Libuv callback to allocate memory for an incoming client read
static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  buf->base = malloc(suggested_size);
  buf->len = suggested_size;
}


#define MAX_CLIENT_BUF_SIZE (16 * 1024 * 1024) // 16 MB
static void on_client_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  client_t *client = (client_t *)stream->data;
  struct Server *s = client->server; // Need reference to server for flushing

  if (nread > 0) {
    // Resize buffer if necessary
    if (client->buffer_len + nread > client->buffer_cap) {
      // Logic to prevent infinite growth or handle OOM could go here
      size_t new_cap = (client->buffer_len + nread) * 2;

      char *new_buf = realloc(client->buffer, new_cap);
      if (!new_buf) {
        fprintf(stderr, "Critical: OOM in buffer realloc\n");
        free(buf->base);
        close_and_free_client(client);
        return;
      }
      client->buffer = new_buf;
      client->buffer_cap = new_cap;
    }

    // Append new data
    memcpy(client->buffer + client->buffer_len, buf->base, nread);
    client->buffer_len += nread;

    if (client->buffer_len > MAX_CLIENT_BUF_SIZE) {
      fprintf(stderr, "Client exeeded memory limit. Disconnecting.\n");
      close_and_free_client(client);
      free(buf->base);
      return;
    }

    // Process available messages
    process_insert_buffer(client);

  } else if (nread < 0) {
    // The client sent FIN, but we might still have requests in client->buffer 
    // that we buffered during flow control.
    if (client->buffer_len > 0) {
      printf("EOF received. Processing remaining %lu bytes in buffer.\n", client->buffer_len);
      process_insert_buffer(client);
    }

    // Flush any partial Raft batches
    if (client->ctx.insert.batched_req_count > 0) {
      printf("Flushing final batch of %u items due to client disconnect.\n", client->ctx.insert.batched_req_count);
      batch_buffer_flush(s, client);
    }

    // Error or EOF
    if (nread != UV_EOF) {
      fprintf(stderr, "Read error: %s\n", uv_strerror(nread));
    } else {
      printf("Client disconnected (EOF).\n");
    }

    printf("CLOSING\n");
    close_and_free_client(client);
  }

  if (buf->base) free(buf->base);
}

// Returns true if client is still active, false if closed/freed.
static bool process_insert_buffer(client_t *client) {
  struct Server *s = client->server;
  size_t offset = 0;
  client_insert_ctx_t *ins_ctx = &client->ctx.insert;

  // Loop as long as we might have a complete message in the buffer
  while (offset + sizeof(uint32_t) <= client->buffer_len) {

    // Read the length of the message payload
    uint32_t payload_len;
    memcpy(&payload_len, client->buffer + offset, sizeof(uint32_t));

    uint32_t total_msg_len = payload_len;

    // A message needs at least a 4-byte length prefix
    if (total_msg_len < sizeof(uint32_t)) {
      close_and_free_client(client);
      return false;
    }

    // Do we have the full message in our buffer?
    if (offset + total_msg_len > client->buffer_len) {
      break; // Incomplete message, wait for more data
    }

    void *msg_ptr = client->buffer + offset;
    //Logf(s->id, "TCP: Received command with size %u", payload_len);

    if (s->raft.state != RAFT_LEADER) {
      Log(s->id, "TCP: Rejecting command, not the leader.");
      //sendback the current leader ip:port.
      redirect_to_leader_and_close(client);
      //close_and_free_client(client);
      return false;
    } else {

      /* Just for metrics. */
      if (ins_ctx->batched_req_count == 0) {
        ins_ctx->batch_start_ts = uv_now(s->loop);
      }

      /* Add to Batch */
      if (ins_ctx->batch_offset + total_msg_len <= ins_ctx->buffer_capacity) {
        memcpy((uint8_t*)ins_ctx->batch_buffer + ins_ctx->batch_offset, msg_ptr,
            total_msg_len);
        ins_ctx->batch_offset += total_msg_len;
        ins_ctx->batched_req_count++;

        /* Stats. */
        //s->stats.total_received++;

        //printf("[MSG BATCHED] payload_len=%u\n", total_msg_len);
      } else {
        Logf(s->id, "Error: Message too large for batch buffer (%u > %u)", 
            total_msg_len, ins_ctx->buffer_capacity);
        close_and_free_client(client);
        return false;
      }
      /* Check if we need to FLUSH after adding this new message.
       * Flush if:
       *  a) Batch count limit reached
       *  b) Batch buffer size limit reached (can't fit new message)
       */
      bool batch_full_count = (ins_ctx->batched_req_count >= BATCH_SIZE);
      bool batch_full_size  = (ins_ctx->batch_offset >= ins_ctx->buffer_capacity);

      if (batch_full_count || batch_full_size) {
        batch_buffer_flush(s, client);
      }
    }
    offset += total_msg_len;
  }

  // Only move memory after we are done reading everything possible.
  if (offset > 0) {
    size_t remaining = client->buffer_len - offset;
    if (remaining > 0) {
      memmove(client->buffer, client->buffer + offset, remaining);
    }
    client->buffer_len = remaining;
  }

  /* 
   * If we have processed everything in the buffer (buffer_len is 0),
   * but we have pending items in the batch, 
   * we MUST flush now. The client is waiting for these ACKs.
   */
  if (client->buffer_len == 0 && ins_ctx->batched_req_count > 0) {
    batch_buffer_flush(s, client);
  }
  return true;
}


// Libuv callback for when a new client connects to our server
static void on_insert_connection(uv_stream_t *server_handle, int status) {
  if (status < 0) {
    fprintf(stderr, "New connection error: %s\n", uv_strerror(status));
    return;
  }

  struct Server *s = (struct Server *)server_handle->data;

  // Allocate and initialize a new client struct
  client_t *client = calloc(1, sizeof(client_t));
  if (!client) { /* handle OOM */ return; }
  client->type = CLIENT_TYPE_INSERT;

  client->server = s;
  client->handle.data = client; 
  client->closing = false;

  client->ctx.insert.batch_buffer = calloc(BATCH_SIZE, sizeof(insert_cmd_t));
  client->ctx.insert.batch_offset = 0;
  client->ctx.insert.batched_req_count = 0;
  client->ctx.insert.buffer_capacity = BATCH_SIZE * sizeof(insert_cmd_t);

  uv_tcp_init(s->loop, &client->handle);


  if (uv_accept(server_handle, (uv_stream_t *)&client->handle) == 0) {
    client_retain(client);

    if (s->raft.state != RAFT_LEADER) {
      Log(s->id, "TCP: Rejecting insert client (Not Leader).");
      redirect_to_leader_and_close(client);
      return;
    }

    Log(s->id, "TCP: New insert client connected.");
    add_client(s, client, CLIENT_TYPE_INSERT);

    uv_tcp_keepalive(&client->handle, 1, 60);
    uv_tcp_nodelay(&client->handle, 1); 
    uv_read_start((uv_stream_t *)&client->handle, alloc_cb, on_client_read);
  }
  else {
    client_retain(client);
    close_and_free_client(client);
  }
}

static void on_client_query(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  client_t *client = (client_t *)stream->data;

  if (nread > 0) {
    if (client->buffer_len + nread > client->buffer_cap) {
      // Logic to prevent infinite growth or handle OOM could go here
      size_t new_cap = (client->buffer_len + nread) * 2;
      char *new_buf = realloc(client->buffer, new_cap);
      if (!new_buf) {
        fprintf(stderr, "Critical: OOM in buffer realloc\n");
        free(buf->base);
        close_and_free_client(client);
        return;
      }
      client->buffer = new_buf;
      client->buffer_cap = new_cap;
    }

    memcpy(client->buffer + client->buffer_len, buf->base, nread);
    client->buffer_len += nread;
    dispatch_queries(client);
  }
  else if (nread < 0) {
    // Error or EOF
    if (nread != UV_EOF) {
      fprintf(stderr, "Read error: %s\n", uv_strerror(nread));
    } else {
      printf("Client disconnected (EOF).\n");
    }

    printf("CLOSING\n");
    close_and_free_client(client);
  }
  if (buf->base) free(buf->base);
}

static void on_read_connection(uv_stream_t *server_handle, int status) {
  if (status < 0) {
    fprintf(stderr, "New connection error: %s\n", uv_strerror(status));
    return;
  }

  struct Server *s = (struct Server *)server_handle->data;

  // Allocate and initialize a new client struct
  client_t *client = calloc(1, sizeof(client_t));
  if (!client) { /* handle OOM */ return; }

  client->type = CLIENT_TYPE_QUERY;
  client->server = s;
  client->handle.data = client; 
  client->closing = false;

  uv_tcp_init(s->loop, &client->handle);
  if (uv_accept(server_handle, (uv_stream_t *)&client->handle) == 0) {
    client_retain(client);

    if (s->raft.state == RAFT_LEADER) {
      Log(s->id, "TCP: Rejecting query client (I'm the Leader).");
      close_and_free_client(client);
      return;
    }

    Log(s->id, "TCP: New query client connected.");
    client->ctx.query.total_queries = 0;
    client->ctx.query.active_query_start_ts = 0;

    add_client(s, client, CLIENT_TYPE_QUERY);

    uv_tcp_keepalive(&client->handle, 1, 60);
    uv_tcp_nodelay(&client->handle, 1); 
    uv_read_start((uv_stream_t *)&client->handle, alloc_cb, on_client_query);
  } else {
    client_retain(client);
    close_and_free_client(client);
  }
}

static int client_uv_init(struct Server *s, uv_tcp_t *tcp_handle,
    int port, uv_connection_cb on_connection) {
  int rv;
  struct sockaddr_in addr;

  Log(s->id, "Setting up TCP connection.");
  uv_tcp_init(s->loop, tcp_handle);
  tcp_handle->data = s; 

  // Listen on port 7000 + server_id (e.g., 7001, 7002, 7003)
  uv_ip4_addr("0.0.0.0", port, &addr); 

  rv = uv_tcp_bind(tcp_handle, (const struct sockaddr*)&addr, 0);
  if (rv != 0) {
    Logf(s->id, "uv_tcp_bind(): %s", uv_strerror(rv));
    // Add proper cleanup here if other handles were init'd
    return -1;
    //goto err;
  }

  rv = uv_listen((uv_stream_t*)tcp_handle, 128, on_connection);
  if (rv != 0) {
    Logf(s->id, "uv_listen(): %s", uv_strerror(rv));
    return -2;
    //goto err;
  }
  Logf(s->id, "TCP server listening on port %d", port);
  return 0;
}

static int tcp_setup(struct Server *s, int port) {
  int rv;

  rv = client_uv_init(s, &s->tcp_write_handle, port, on_insert_connection);
  if (rv != 0) return rv;

  rv = client_uv_init(s, &s->tcp_read_handle, port + 1000, on_read_connection);
  return rv;
}

typedef struct bootstrap_node {
  const char *addr;
  int id;
  int port;
} bootstrap_node_t;


static void setup_high_level_stats(struct Server * s, const char *path) {
  s->stats.f = fopen(path, "w");
  if (s->stats.f == NULL) {
    perror("Failed to init stats file.");
    exit(1);
  } 

  fprintf(s->stats.f, "Timestamp_ms,Role,Term,Write_OPS,Write_MBps,Read_OPS,Read_MBps,PendingRequests,PendingBytes,Backlog,Avg_Latency_ms,P50_Latency_ms,P95_Latency_ms,P99_Latency_ms,Max_Latency_ms,Raft_Idx_Local,Raft_Idx_Applied,Raft_Idx_Commit\n"); 
  fflush(s->stats.f);

  s->stats.total_requests = 0;
  s->stats.total_bytes = 0;
  s->stats.prev_requests = 0;
  s->stats.prev_bytes = 0;
  //s->stats.total_received = 0;
  s->stats.last_run_time = 0;
  s->stats.period_latency_sum = 0;
  s->stats.period_batches_count = 0; 
  
  /* Initialize read stats */
  s->read_stats.total_queries = 0;
  s->read_stats.total_bytes_read = 0;
  s->read_stats.prev_queries = 0;
  s->read_stats.prev_bytes_read = 0;
  
  /* Initialize latency buffer */
  latency_buffer_init(&s->latency_buffer);
}

static void setup_storage_events_file(struct Server *s, const char *path) {
  s->storage_events_file = fopen(path, "w");
  if (!s->storage_events_file) {
    printf("Failed to open storage events file %s\n", path);
    exit(1);
  }
  
  /* Write header */
  fprintf(s->storage_events_file, "event_type,timestamp,duration_ms");
  fprintf(s->storage_events_file, ",quantity_merged_tables,input_bytes,output_bytes,level");
  fprintf(s->storage_events_file, ",bytes_flushed\n");
  fflush(s->storage_events_file);
  
  /* Initialize buffer */
  s->storage_events_count = 0;
}

/* Initialize the example server struct, without starting it yet. */
static int ServerInit(struct Server *s,
    struct uv_loop_s *loop,
    const char *dir,
    cluster_config_t *cluster_conf,
    uint32_t id)
{
  struct raft_configuration configuration;
  struct timespec now;
  int rv;

  /* The configuration for the current node. */
  node_config_t *node_config = NULL;

  for (int i = 0; i < cluster_conf->count; i++) {
    if (cluster_conf->nodes[i].id == id) {
      node_config = &cluster_conf->nodes[i];
      break;
    }
  }
  if (!node_config) {
    fprintf(stderr, "Error: ID %u not found in configuration file.\n", id);
    exit(1);
  }

  memset(s, 0, sizeof *s);

  /* Seed the random generator */
  timespec_get(&now, TIME_UTC);
  srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

  /* Allocate the batch request buffer. */
  /*
     s->batch_buffer = calloc(BATCH_SIZE, sizeof(insert_cmd_t));
     s->batch_offset = 0;
     s->batched_req_count = 0;
     s->buffer_capacity = BATCH_SIZE * sizeof(insert_cmd_t);
     */
  s->loop = loop;
  s->last_state = -1;

  /* Add a timer to periodically try to propose a new entry. */
  rv = uv_timer_init(s->loop, &s->timer);
  if (rv != 0) {
    Logf(s->id, "uv_timer_init(): %s", uv_strerror(rv));
    goto err;
  }
  s->timer.data = s;

  /* Initialize the TCP-based RPC transport. */
  s->transport.version = 1;
  s->transport.data = NULL;
  rv = raft_uv_tcp_init(&s->transport, s->loop);
  if (rv != 0) {
    goto err;
  }

  /* Initialize the libuv-based I/O backend. */
  rv = raft_uv_init(&s->io, s->loop, dir, &s->transport);
  if (rv != 0) {
    Logf(s->id, "raft_uv_init(): %s", s->io.errmsg);
    goto err_after_uv_tcp_init;
  }

  /* Initialize the finite state machine. */
  rv = FsmInit(&s->fsm, s);
  if (rv != 0) {
    Logf(s->id, "FsmInit(): %s", raft_strerror(rv));
    goto err_after_uv_init;
  }

  /* Save the server ID. */
  s->id = id;
  s->active_queries_count = 0;

  /* Render the address. */
  strcpy(s->address, node_config->raft_address);

  /* Initialize and start the engine, using the libuv-based I/O backend. */
  rv = raft_init(&s->raft, &s->io, &s->fsm, id, s->address);
  if (rv != 0) {
    Logf(s->id, "raft_init(): %s", raft_errmsg(&s->raft));
    goto err_after_fsm_init;
  }
  s->raft.data = s;


  /* Bootstrap the initial configuration if needed. */
  raft_configuration_init(&configuration);

  for (int i = 0; i < cluster_conf->count; i++) {
    node_config_t *node = &cluster_conf->nodes[i];
    rv = raft_configuration_add(&configuration, node->id, node->raft_address,
        RAFT_VOTER); 
    if (rv != 0) {
      Logf(s->id, "raft_configuration_add(): %s", raft_strerror(rv));
      goto err_after_configuration_init;
    }
  }

  rv = raft_bootstrap(&s->raft, &configuration);
  if (rv != 0 && rv != RAFT_CANTBOOTSTRAP) {
    goto err_after_configuration_init;
  }
  raft_configuration_close(&configuration);

  raft_set_snapshot_threshold(&s->raft, UINT32_MAX); //64);
  raft_set_snapshot_trailing(&s->raft, 16);
  raft_set_pre_vote(&s->raft, true);

  //raft_set_election_timeout(&s->raft, 5000);   // 5 Seconds
  //raft_set_heartbeat_timeout(&s->raft, 500);   // 0.5 Seconds

  /* Allow more data in flight to fill. 
   * With 4KB batches, 256 entries = 1MB of in-flight data. */
  raft_set_max_inflight_entries(&s->raft, 256);

  s->transfer.data = s;

  /* Setup tcp connection for handling incoming requests. */
  rv = tcp_setup(s, node_config->client_port);
  if (rv != 0) {
    exit(1);
  }

  s->db = lsmt_init(dir);
  db = s->db;

  /* Setup statistics collection. */
  uint64_t file_ts = uv_now(s->loop);
  char stats_file_path[256];
  sprintf(stats_file_path, "%s/stats_%d_%lu.csv", dir, s->id, file_ts); 
  setup_high_level_stats(s, stats_file_path);
 
  /* Setup storage events file */
  sprintf(stats_file_path, "%s/storage_events_%d_%lu.csv", dir, s->id, file_ts);
  setup_storage_events_file(s, stats_file_path);
  
  uv_mutex_init(&s->storage_events_mutex);

  /* Register storage event callbacks with lsmt */
  lsmt_set_compaction_callback(s->db, on_compaction_event, s);
  lsmt_set_memtable_flush_callback(s->db, on_memtable_flush_event, s);
  
  return 0;

err_after_configuration_init:
  raft_configuration_close(&configuration);
err_after_fsm_init:
  FsmClose(&s->fsm);
err_after_uv_init:
  raft_uv_close(&s->io);
err_after_uv_tcp_init:
  raft_uv_tcp_close(&s->transport);
err:
  return rv;
}

static void update_batch_metrics(struct Server *s, apply_ctx_t *ctx) {
  /* Calculate Latency */
  uint64_t now = uv_now(s->loop);

  uint64_t batch_time = now - ctx->batch_creation_ts;

  /* Store latency in buffer for distribution calculation */
  latency_buffer_add(&s->latency_buffer, batch_time);

  s->stats.period_latency_sum += batch_time;
  s->stats.period_batches_count++;
}

/* Called after a request to apply a new command to the FSM has been
 * completed. */
static void serverApplyCb(struct raft_apply *req, int status, void *result)
{
  apply_ctx_t *ctx = (apply_ctx_t *)req->data;
  struct Server *s = ctx->server;
  client_t *c = ctx->client;

  if (status == 0) {
    update_batch_metrics(s, ctx);
  }

  if (status == 0 && c != NULL && ! c->closing) {

    ack_write_t *wr = malloc(sizeof(ack_write_t));
    if (wr) {
      wr->req.data = wr;
      wr->client = c;
      client_retain(c);

      int count = ctx->req_count > BATCH_SIZE ? BATCH_SIZE : ctx->req_count;
      wr->buf = uv_buf_init(ACK_BUFFER, count);

      int r = uv_write(&wr->req, (uv_stream_t*)&c->handle, &wr->buf, 1, on_ack_write_complete);

      if (r != 0) {
        client_release(c);
        free(wr);
        close_and_free_client(c);
      }
    }
  }
  client_release(c);
  if (status != 0) {
    if (status != RAFT_LEADERSHIPLOST) {
      Logf(s->id, "raft_apply() callback: %s (%d)", raft_errmsg(&s->raft),
          status);
    }
  }

  free(ctx);
  raft_free(req); 
}

static void statsTimerCb(uv_timer_t *timer)
{
  struct Server *s = timer->data;
  uint64_t epoch_ms = get_unix_epoch();
  uint64_t now = uv_now(s->loop);

  int current_state = s->raft.state;
  raft_term term = raft_current_term(&s->raft);

  /* -----------------------------------------------------------------------
   * Calculate Time Delta 
   * ----------------------------------------------------------------------- */
  if (s->stats.last_run_time == 0) {
    s->stats.last_run_time = now - APPLY_RATE; 
  }

  uint64_t dt = now - s->stats.last_run_time;
  if (dt == 0) dt = 1;

  /* -----------------------------------------------------------------------
   * Calculate Write Rates (OPS & MB/s)
   * ----------------------------------------------------------------------- */
  uint64_t current_reqs = s->stats.total_requests;
  uint64_t current_bytes = s->stats.total_bytes;

  uint64_t delta_reqs = current_reqs - s->stats.prev_requests;
  uint64_t delta_bytes = current_bytes - s->stats.prev_bytes;

  /* Normalize to Seconds */
  uint64_t write_ops_sec = (delta_reqs * 1000) / dt;
  double write_mb_sec = ((double)delta_bytes / (1024.0 * 1024.0)) * (1000.0 / dt);

  /* -----------------------------------------------------------------------
   * Calculate Read Rates (OPS & MB/s)
   * ----------------------------------------------------------------------- */
  uint64_t current_queries = s->read_stats.total_queries;
  uint64_t current_bytes_read = s->read_stats.total_bytes_read;

  uint64_t delta_queries = current_queries - s->read_stats.prev_queries;
  uint64_t delta_bytes_read = current_bytes_read - s->read_stats.prev_bytes_read;

  uint64_t read_ops_sec = (delta_queries * 1000) / dt;
  double read_mb_sec = ((double)delta_bytes_read / (1024.0 * 1024.0)) * (1000.0 / dt);

  /* -----------------------------------------------------------------------
   * Calculate Average Latency (From previous step)
   * ----------------------------------------------------------------------- */
  double avg_latency_ms = 0.0;
  if (s->stats.period_batches_count > 0) {
    avg_latency_ms = (double)s->stats.period_latency_sum / (double)s->stats.period_batches_count;
  }

  /* -----------------------------------------------------------------------
   * Calculate Latency Distribution (p50, p95, p99, MAX)
   * ----------------------------------------------------------------------- */
  double p50_latency_ms = 0.0;
  double p95_latency_ms = 0.0;
  double p99_latency_ms = 0.0;
  double max_latency_ms = 0.0;
  
  if (s->latency_buffer.count > 0) {
    /* Copy and sort samples */
    uint64_t sorted_samples[MAX_LATENCY_SAMPLES];
    size_t count = s->latency_buffer.count;
    size_t start = (s->latency_buffer.head + MAX_LATENCY_SAMPLES - count) % MAX_LATENCY_SAMPLES;
    
    for (size_t i = 0; i < count; i++) {
      sorted_samples[i] = s->latency_buffer.samples[(start + i) % MAX_LATENCY_SAMPLES];
    }
    
    /* Insertion sort for small arrays */
    for (size_t i = 1; i < count; i++) {
      uint64_t key = sorted_samples[i];
      size_t j = i;
      while (j > 0 && sorted_samples[j - 1] > key) {
        sorted_samples[j] = sorted_samples[j - 1];
        j--;
      }
      sorted_samples[j] = key;
    }
    
    p50_latency_ms = calculate_percentile(sorted_samples, count, 50.0);
    p95_latency_ms = calculate_percentile(sorted_samples, count, 95.0);
    p99_latency_ms = calculate_percentile(sorted_samples, count, 99.0);
    max_latency_ms = (double)sorted_samples[count - 1];
  }

  raft_index idx_current = raft_last_index(&s->raft);
  raft_index idx_commit  = raft_commit_index(&s->raft);
  raft_index idx_applied = raft_last_applied(&s->raft);

  /* -----------------------------------------------------------------------
   * Calculate Backlog 
   * ----------------------------------------------------------------------- */
  uint64_t pending_client_reqs = 0;
  uint64_t total_pending_bytes = 0;
  client_t *c = s->insert_clients_head;
  while (c != NULL) {
    client_t *next = c->next;
    pending_client_reqs += c->ctx.insert.batched_req_count;
    total_pending_bytes += c->ctx.insert.batch_offset;

    // Also flush stale data here
    batch_buffer_flush(s, c);
    c = next;
  }

  /* 
   * Raft Lag: Entries that are committed by consensus but not yet applied 
   * to your LSM Tree. This indicates disk I/O bottlenecks in lsmt_insert.
   */
  uint64_t raft_lag = (idx_commit > idx_applied) ? (idx_commit - idx_applied) : 0;
  uint64_t backlog = pending_client_reqs + raft_lag;

  /* -----------------------------------------------------------------------
   * Update State & Log
   * ----------------------------------------------------------------------- */
  const char *role_name = "UNKNOWN";
  if (current_state == RAFT_LEADER) role_name = "LEADER";
  else if (current_state == RAFT_FOLLOWER) role_name = "FOLLOWER";
  else if (current_state == RAFT_CANDIDATE) role_name = "CANDIDATE";

  if (s->stats.f) {
    fprintf(s->stats.f, "%lu,%s,%llu,%lu,%.2f,%lu,%.2f,%lu,%lu,%lu,%.2f,%.2f,%.2f,%.2f,%.2f,%llu,%llu,%llu\n",
        epoch_ms, 
        role_name, 
        term,
        write_ops_sec, 
        write_mb_sec,
        read_ops_sec,
        read_mb_sec,
        pending_client_reqs, 
        total_pending_bytes, 
        backlog,
        avg_latency_ms,
        p50_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        max_latency_ms,
        idx_current,
        idx_applied,
        idx_commit
        );
  }
  
  // Reset Counters
  s->stats.prev_requests = s->stats.total_requests;
  s->stats.prev_bytes = s->stats.total_bytes;
  s->stats.period_latency_sum = 0;
  s->stats.period_batches_count = 0;
  s->stats.last_run_time = now;
  
  /* Reset read counters */
  s->read_stats.prev_queries = s->read_stats.total_queries;
  s->read_stats.prev_bytes_read = s->read_stats.total_bytes_read;
  
  /* Flush storage events periodically (every 10 seconds = 100 * 100ms) */
  static uint64_t last_storage_flush = 0;
  if (now - last_storage_flush >= 10000) {
    storage_events_flush(s);
    last_storage_flush = now;
  }

  /* Reset the latency buffer so we don't carry over old latencies */
  latency_buffer_init(&s->latency_buffer);
}

/* Called periodically every APPLY_RATE milliseconds. */
/* Start the example server. */
static int ServerStart(struct Server *s)
{
  int rv;

  Log(s->id, "starting");

  rv = raft_start(&s->raft);
  if (rv != 0) {
    Logf(s->id, "raft_start(): %s", raft_errmsg(&s->raft));
    goto err;
  }

  rv = uv_timer_start(&s->timer, statsTimerCb, 0, APPLY_RATE);
  if (rv != 0) {
    Logf(s->id, "uv_timer_start(): %s", uv_strerror(rv));
    goto err;
  }

  return 0;

err:
  return rv;
}

/* Release all resources used by the example server. */
static void ServerClose(struct Server *s, ServerCloseCb cb)
{
  s->close_cb = cb;
  Log(s->id, "stopping");
  //lsmt_free(s->db);

  /* Close the timer asynchronously if it was successfully
   * initialized. Otherwise invoke the callback immediately. */
  if (s->timer.data != NULL) {
    uv_close((struct uv_handle_s *)&s->timer, serverTimerCloseCb);
  } else {
    s->close_cb(s);
  }

  /* Flush metrics to disk. */
  if (s->stats.f) {
    fclose(s->stats.f);
    s->stats.f = NULL;
  }

 
  /* Flush and close storage events file */
  if (s->storage_events_file) {
    storage_events_flush(s);
    fclose(s->storage_events_file);
    s->storage_events_file = NULL;
    uv_mutex_destroy(&s->storage_events_mutex);
  }

  /* Close TCP Listening Sockets */
  if (s->loop) {
    if (!uv_is_closing((uv_handle_t*)&s->tcp_write_handle)) {
      uv_close((uv_handle_t*)&s->tcp_write_handle, NULL);
    }
    if (!uv_is_closing((uv_handle_t*)&s->tcp_read_handle)) {
      uv_close((uv_handle_t*)&s->tcp_read_handle, NULL);
    }
  }

  /* Close All Active Insert Clients */
  while (s->insert_clients_head != NULL) {
      close_and_free_client(s->insert_clients_head);
  }

  /* Close All Active Query Clients */
  while (s->query_clients_head != NULL) {
      close_and_free_client(s->query_clients_head);
  }

  if (s->db) {
    lsmt_flush(s->db);
  }
  Log(s->id, "Shutted down.");
}

/********************************************************************
 *
 * Top-level main loop.
 *
 ********************************************************************/

static void mainServerCloseCb(struct Server *server)
{
  struct uv_signal_s *sigint = server->data;
  uv_close((struct uv_handle_s *)sigint, NULL);
}

/* Handler triggered by SIGINT. It will initiate the shutdown sequence. */
static void mainSigintCb(struct uv_signal_s *handle, int signum)
{
  (void)signum;
  struct Server *server = handle->data;
  assert(signum == SIGINT);
  uv_signal_stop(handle);
  server->data = handle;
  ServerClose(server, mainServerCloseCb);
}

int main(int argc, char *argv[])
{
  memset(ACK_BUFFER, 1, BATCH_SIZE);

  struct uv_loop_s loop;
  struct uv_signal_s sigint; /* To catch SIGINT and exit. */
  struct Server server;
  const char *dir;
  const char *conf_path;
  uint32_t id;
  int rv;


  if (argc != 4) {
    printf("usage: server <data_dir> <id> <cluster.conf>\n");
    return 1;
  }

  dir = argv[1];
  id = (uint32_t)atoi(argv[2]);
  conf_path = argv[3];
  cluster_config_t cluster_conf = {0};

  if (load_cluster_config(conf_path, &cluster_conf) != 0) {
    fprintf(stderr, "Failed to laod the cluster config from %s\n", conf_path);
    return 1;
  }

  /* Ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254 */
  signal(SIGPIPE, SIG_IGN);

  /* Initialize the libuv loop. */
  rv = uv_loop_init(&loop);
  if (rv != 0) {
    Logf(id, "uv_loop_init(): %s", uv_strerror(rv));
    goto err;
  }

  /* Initialize the example server. */
  rv = ServerInit(&server, &loop, dir, &cluster_conf, id);
  if (rv != 0) {
    goto err_after_server_init;
  }

  /* Add a signal handler to stop the example server upon SIGINT. */
  rv = uv_signal_init(&loop, &sigint);
  if (rv != 0) {
    Logf(id, "uv_signal_init(): %s", uv_strerror(rv));
    goto err_after_server_init;
  }
  sigint.data = &server;
  rv = uv_signal_start(&sigint, mainSigintCb, SIGINT);
  if (rv != 0) {
    Logf(id, "uv_signal_start(): %s", uv_strerror(rv));
    goto err_after_signal_init;
  }

  /* Start the server. */
  rv = ServerStart(&server);
  if (rv != 0) {
    goto err_after_signal_init;
  }

  /* Run the event loop until we receive SIGINT. */
  rv = uv_run(&loop, UV_RUN_DEFAULT);
  if (rv != 0) {
    Logf(id, "uv_run_start(): %s", uv_strerror(rv));
  }

  uv_loop_close(&loop);

  return rv;

err_after_signal_init:
  uv_close((struct uv_handle_s *)&sigint, NULL);
err_after_server_init:
  ServerClose(&server, NULL);
  uv_run(&loop, UV_RUN_DEFAULT);
  uv_loop_close(&loop);
err:
  return rv;
}

/* 
 * RUNS ON MAIN EVENT LOOP WHEN THREAD IS DONE
 * Safely sends the memory buffer via the TCP socket.
 */
static void after_query_cb(uv_work_t *req, int status) {
  query_task_t *task = (query_task_t *)req->data;
  client_t *client = task->client;
  struct Server *s = task->client->server;

  if (status == 0 && !client->closing && task->response_msg) {
    uint64_t t_send_start = uv_hrtime();

    // Note: send_response frees the buffer automatically on failure
    if (send_response(client, task->response_msg, task->total_msg_size) != 0) {
      printf("FAILED SEND QUERY RESPONSE.\n");
    } else {
      double send_us = (uv_hrtime() - t_send_start) / 1000.0;
      double total_ms = (uv_hrtime() - task->t_start_ns) / 1000000.0;

      printf("[Profile] Reqs: 1 | Records: %lu | Total: %.3f ms | Iter: %lu us | Send: %.0f us\n", 
          task->records_count, total_ms, task->t_iter_us, send_us);
    }
  } else {
    // If the client closed while the thread was running, free the buffer manually
    if (task->response_msg) {
      free(task->response_msg);
    }
  }

  s->active_queries_count--;
  if (s->active_queries_count < GLOBAL_MAX_QUERIES) {
    client_t *c = s->query_clients_head;
    while (c != NULL) {
      if (c->ctx.query.is_paused && !c->closing) {
        c->ctx.query.is_paused = false;
        uv_read_start((uv_stream_t *)&c->handle, alloc_cb, on_client_query);
        dispatch_queries(c); 

        if (s->active_queries_count >= GLOBAL_MAX_QUERIES) break;
      }
      c = c->next;
    }
  }
  client_release(client);
  free(task);
}

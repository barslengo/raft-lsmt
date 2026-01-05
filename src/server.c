/*
 * TODO: FIX Backlogs underflows in stats.
 * TODO: Reorganize folder structure (Raft logs, sstables, etc..).
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <raft/raft.h>
#include <raft/raft/uv.h>

#include <lsmt/lsmt.h>

#define APPLY_RATE 100 /* Store new statistic entry every 100 ms. */

#define Log(SERVER_ID, FORMAT) printf("%d: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
  printf("%d: " FORMAT "\n", SERVER_ID, __VA_ARGS__)


#define BATCH_SIZE 4096 //1024 //128
#define CONTENT_MAX_SIZE 255

static char ACK_BUFFER[BATCH_SIZE];
const uint8_t CONTENT_HEADER_SIZE = sizeof(sl_uint128_t) + sizeof(uint8_t); 
const uint32_t INSERT_CMD_SIZE = CONTENT_HEADER_SIZE + sizeof(uint8_t)*CONTENT_MAX_SIZE; 


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

  /* Batch buffer assigned to the client. */
  void *batch_buffer;                 /* Buffer for batching requests into a single log. */
  uint32_t batch_offset;
  uint32_t buffer_capacity;
  uint32_t batched_req_count;


  bool closing;
  int ref_count;

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
  uint64_t start_time;
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
  struct {
    FILE *f;
    uint64_t total_requests;
    uint64_t total_bytes;

    uint64_t prev_requests;
    uint64_t prev_bytes;

    uint64_t total_received; /* Total requests received from tcp connections. */
    uint64_t last_run_time; 
    uint64_t period_latency_sum; /* Sum of ms taken by all batches in this tick */
    uint64_t period_batches_count; /* How many batches finished in this tick */
  } stats;
};


void client_retain(client_t *c) {
  c->ref_count++;
}

void client_release(client_t *c) {
  c->ref_count--;
  if (c->ref_count == 0) {
    if (c->buffer) free(c->buffer);
    if (c->batch_buffer) free(c->batch_buffer); 
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
  } else {
    *head_ptr = c->next;
  }
  if (c->next) {
    c->next->prev = c->prev;
  }

  c->next = NULL;
  c->prev = NULL;
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
      return RAFT_INVALID;
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
  raft_free(buf->base);
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

static void on_client_close(uv_handle_t *handle);
static bool process_insert_buffer(client_t *client);

// Helper function to safely close and free a client's resources
static void close_and_free_client(client_t *client) {
  if (client->closing) return;
  client->closing = true;

  remove_client(client->server, client);
  uv_close((uv_handle_t *)&client->handle, on_client_close);
}

// Callback that fires after a client's handle is fully closed
static void on_client_close(uv_handle_t *handle) {
  client_t *client = (client_t *)handle->data;
  client_release(client);
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

static void on_ack_write_complete(uv_write_t *req, int status) {
  ack_write_t *wr = (ack_write_t *)req;
  if (status) {
    close_and_free_client(wr->client);
  }

  client_release(wr->client);
  free(wr);
}

static void batch_buffer_flush(struct Server *s, client_t *c) {
  if (c->batched_req_count <= 0) {
    return;
  }

  struct raft_buffer raft_buf;

  uint32_t aligned_msg_size = (c->batch_offset + 7) & ~0x07;
  raft_buf.len = aligned_msg_size;
  raft_buf.base = raft_malloc(raft_buf.len);

  if (!raft_buf.base) {
    fprintf(stderr, "Critical: Out of memory in flush_batch\n");
    exit(1);
  }

  /* Copy from the reusable batch buffer to the Raft entry buffer */
  memset(raft_buf.base, 0, raft_buf.len);
  memcpy(raft_buf.base, c->batch_buffer, c->batch_offset);

  struct raft_apply *req = raft_malloc(sizeof(*req));
  if (!req) {
    fprintf(stderr, "Critical: Out of memory for req in flush_batch\n");
    exit(1);
  }

  apply_ctx_t *ctx = malloc(sizeof(apply_ctx_t));
  ctx->server = s;
  ctx->client = c;
  client_retain(c);
  ctx->req_count = c->batched_req_count;
  ctx->start_time = uv_now(s->loop); 

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
  c->batch_offset = 0;
  c->batched_req_count = 0;
  memset(c->batch_buffer, 0, c->buffer_capacity); // Optional: clear buffer for debug
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
    if (client->batched_req_count > 0) {
      printf("Flushing final batch of %u items due to client disconnect.\n", client->batched_req_count);
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
      close_and_free_client(client);
      return false;
    } else {
      /* Add to Batch */
      if (client->batch_offset + total_msg_len <= client->buffer_capacity) {
        memcpy((uint8_t*)client->batch_buffer + client->batch_offset, msg_ptr,
            total_msg_len);
        client->batch_offset += total_msg_len;
        client->batched_req_count++;

        /* Stats. */
        s->stats.total_received++;

        //printf("[MSG BATCHED] payload_len=%u\n", total_msg_len);
      } else {
        Logf(s->id, "Error: Message too large for batch buffer (%u > %u)", 
            total_msg_len, client->buffer_capacity);
        close_and_free_client(client);
        return false;
      }
      /* Check if we need to FLUSH after adding this new message.
       * Flush if:
       *  a) Batch count limit reached
       *  b) Batch buffer size limit reached (can't fit new message)
       */
      bool batch_full_count = (client->batched_req_count >= BATCH_SIZE);
      bool batch_full_size  = (client->batch_offset >= client->buffer_capacity);

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
  if (client->buffer_len == 0 && client->batched_req_count > 0) {
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

  client->server = s;
  client->handle.data = client; 
  client->closing = false;
  client_retain(client);

  client->batch_buffer = calloc(BATCH_SIZE, sizeof(insert_cmd_t));
  client->batch_offset = 0;
  client->batched_req_count = 0;
  client->buffer_capacity = BATCH_SIZE * sizeof(insert_cmd_t);

  uv_tcp_init(s->loop, &client->handle);

  if (uv_accept(server_handle, (uv_stream_t *)&client->handle) == 0) {
    Log(s->id, "TCP: New client connected.");
    add_client(s, client, CLIENT_TYPE_INSERT);

    uv_tcp_keepalive(&client->handle, 1, 60);
    uv_tcp_nodelay(&client->handle, 1); 
    uv_read_start((uv_stream_t *)&client->handle, alloc_cb, on_client_read);
  } else {
    close_and_free_client(client);
  }
}

/* Serialize the kv_record_t array into the result array.
 * The input array is also freed. */
static uint32_t serialize_records(kv_record_t *records, uint32_t len, uint8_t **result) {
  size_t payload_size = sizeof(uint32_t) + sizeof(uint32_t);

  const size_t key_size = sizeof(sl_uint128_t);
  const size_t data_type_size = sizeof(uint8_t);
  const size_t len_field_size = sizeof(uint32_t);
  const int fixed_size =  key_size + len_field_size + data_type_size;

  for (uint32_t i = 0; i < len; i++) {
    payload_size += fixed_size;
    payload_size += records[i].data_len;
  }

  uint8_t *out = malloc(payload_size);
  if (!out) {
    printf("OOM on serializing data\n");
    exit(1);
  }

  uint32_t offset = 0;

  /* Payload Header. */
  memcpy(out, &payload_size, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(out + offset, &len, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  /* Payload body.
   * Each record is serialized in the following order:
   * [Key|DataType|DataLen|Data]. */
  for (uint32_t i = 0; i < len; i++) {
    memcpy(out + offset, &records[i].key, key_size); 
    offset += key_size;

    memcpy(out + offset, &records[i].data_type, data_type_size);
    offset += data_type_size;

    memcpy(out + offset, &records[i].data_len, len_field_size); 
    offset += len_field_size; 

    memcpy(out + offset, records[i].data, records[i].data_len);
    offset += records[i].data_len;

    free(records[i].data);
  }
  free(records);

  *result = out;
  return payload_size;
}

static void send_records(client_t *c, uint8_t *data, uint32_t payload_size) {
  ack_write_t *wr = malloc(sizeof(ack_write_t));
  if (!wr) {
    free(data); 
    exit(1);
    return;
  }

  wr->req.data = wr;
  wr->client = c;

  // Retain client so it isn't freed while write is pending
  client_retain(c);

  // Cast to char* required by libuv
  wr->buf = uv_buf_init((char*)data, (unsigned int)payload_size);

  //printf("Writing back %u bytes.\n", payload_size);
  int r = uv_write(&wr->req, (uv_stream_t*)&c->handle, &wr->buf, 1, on_query_resp_complete);

  if (r != 0) {
    // Write failed immediately (didn't queue).
    // We must manually perform the cleanup that the callback would have done.
    free(data); 
    client_release(c);
    free(wr);
    close_and_free_client(c);
  }
}

static void process_query_buffer(client_t *client) {
  size_t expected_size = sizeof(sl_uint128_t) * 2; 
  size_t offset = 0;
  size_t remaining = client->buffer_len;

  while (remaining >= expected_size) {
    //printf("Processing query\n");
    sl_uint128_t start_key;
    sl_uint128_t end_key;

    memcpy(&start_key, client->buffer + offset, sizeof(sl_uint128_t)); 
    offset += sizeof(sl_uint128_t);
    memcpy(&end_key, client->buffer + offset, sizeof(sl_uint128_t)); 
    offset += sizeof(sl_uint128_t);

    kv_record_t *records = NULL; // array of records of size 'records_count'.
    //printf("Loading records..\n");
    uint32_t records_count = lsmt_get(db, start_key, end_key, &records);
    //printf("Loaded %u records\n", records_count);

    uint8_t *payload = NULL;
    uint32_t payload_size = serialize_records(records, records_count, &payload);
    //printf("serialized data into a payload of size %u\n", payload_size);
    send_records(client, payload, payload_size);
    remaining -= expected_size;
  }

  // Only move memory after we are done reading everything possible.
  if (offset > 0) {
    if (remaining > 0) {
      memmove(client->buffer, client->buffer + offset, remaining);
    }
    client->buffer_len = remaining;
  }
}

/* Payload format: [Start_Key (16B)|End_Key (16B)]
 * Response format: [Len (32B) | Records]
 */
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
    process_query_buffer(client);
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

  client->server = s;
  client->handle.data = client; 
  client->closing = false;
  client_retain(client);

  //client->batch_buffer = calloc(BATCH_SIZE, sizeof(insert_cmd_t));
  //client->batch_offset = 0;
  //client->batched_req_count = 0;
  //client->buffer_capacity = BATCH_SIZE * sizeof(insert_cmd_t);

  uv_tcp_init(s->loop, &client->handle);

  if (uv_accept(server_handle, (uv_stream_t *)&client->handle) == 0) {
    Log(s->id, "TCP: New client connected.");
    add_client(s, client, CLIENT_TYPE_QUERY);

    uv_tcp_keepalive(&client->handle, 1, 60);
    uv_tcp_nodelay(&client->handle, 1); 
    uv_read_start((uv_stream_t *)&client->handle, alloc_cb, on_client_query);
  } else {
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

  rv = client_uv_init(s, &s->tcp_read_handle, port + 4000, on_read_connection);
  return rv;
}

typedef struct bootstrap_node {
  const char *addr;
  int id;
  int port;
} bootstrap_node_t;

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
  char stats_file_path[128];
  sprintf(stats_file_path, "%s/stats_%d.csv", dir, s->id);
  s->stats.f = fopen(stats_file_path, "w");
  if (s->stats.f == NULL) {
    perror("Failed to init stats file.");
    exit(1);
  } 

  fprintf(s->stats.f, "Timestamp_ms,Role,OPS,Throughput_MBps,Batch_Size,Pending_Queue,Backlog,Avg_Latency_ms\n"); 
  //fprintf(s->stats.f, "Timestamp_ms,Role,OPS,Throughput_MBps,Batch_Size,Pending_Queue,Backlog\n"); 
  fflush(s->stats.f);

  s->stats.total_requests = 0;
  s->stats.total_bytes = 0;
  s->stats.prev_requests = 0;
  s->stats.prev_bytes = 0;
  s->stats.total_received = 0;
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

/* Called after a request to apply a new command to the FSM has been
 * completed. */
static void serverApplyCb(struct raft_apply *req, int status, void *result)
{
  apply_ctx_t *ctx = (apply_ctx_t *)req->data;
  struct Server *s = ctx->server;
  client_t *c = ctx->client;

  /* Calculate Latency */
  uint64_t now = uv_now(s->loop);
  uint64_t latency = now - ctx->start_time;

  /* Update Stats Accumulators */
  s->stats.period_latency_sum += latency;
  s->stats.period_batches_count++;

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
  uint64_t now = uv_now(s->loop);
  int current_state = s->raft.state;

  /* -----------------------------------------------------------------------
   * Calculate Time Delta 
   * ----------------------------------------------------------------------- */
  if (s->stats.last_run_time == 0) {
    s->stats.last_run_time = now - APPLY_RATE; 
  }

  uint64_t dt = now - s->stats.last_run_time;
  if (dt == 0) dt = 1;

  /* -----------------------------------------------------------------------
   * Calculate Rates (OPS & MB/s)
   * ----------------------------------------------------------------------- */
  uint64_t current_reqs = s->stats.total_requests;
  uint64_t current_bytes = s->stats.total_bytes;

  uint64_t delta_reqs = current_reqs - s->stats.prev_requests;
  uint64_t delta_bytes = current_bytes - s->stats.prev_bytes;

  /* Normalize to Seconds */
  uint64_t ops_sec = (delta_reqs * 1000) / dt;
  double mb_sec = ((double)delta_bytes / (1024.0 * 1024.0)) * (1000.0 / dt);

  /* -----------------------------------------------------------------------
   * Calculate Average Latency (From previous step)
   * ----------------------------------------------------------------------- */
  double avg_latency_ms = 0.0;
  if (s->stats.period_batches_count > 0) {
    avg_latency_ms = (double)s->stats.period_latency_sum / (double)s->stats.period_batches_count;
  }

  /* Reset latency accumulators */
  s->stats.period_latency_sum = 0;
  s->stats.period_batches_count = 0;

  /* -----------------------------------------------------------------------
   * Calculate Backlog 
   * ----------------------------------------------------------------------- */
  uint64_t backlog = 0;

  /* 
   * Simple Logic: Input - Output.
   * Safety Check: Ensure we don't underflow if Output > Input 
   * (which happens if we processed logs as a Follower).
   */
  if (s->stats.total_received > s->stats.total_requests) {
    backlog = s->stats.total_received - s->stats.total_requests;
  }

  uint64_t total_pending_reqs = 0;
  uint64_t total_pending_bytes = 0;

  client_t *c = s->insert_clients_head;
  while (c != NULL) {
    client_t *next = c->next;
    total_pending_reqs += c->batched_req_count;
    total_pending_bytes += c->batch_offset;

    /* flush data that's being too long in the buffer. */
    batch_buffer_flush(s, c);
    c = next;
  }
  /* -----------------------------------------------------------------------
   * Update State & Log
   * ----------------------------------------------------------------------- */
  s->stats.prev_requests = current_reqs;
  s->stats.prev_bytes = current_bytes;
  s->stats.last_run_time = now;
  const char *role_name = "UNKNOWN";
  if (current_state == RAFT_LEADER) role_name = "LEADER";
  else if (current_state == RAFT_FOLLOWER) role_name = "FOLLOWER";
  else if (current_state == RAFT_CANDIDATE) role_name = "CANDIDATE";

  if (s->stats.f) {
    fprintf(s->stats.f, "%lu,%s,%lu,%.2f,%lu,%lu,%lu,%.2f\n",
        now, 
        role_name, 
        ops_sec, 
        mb_sec,
        total_pending_reqs, 
        total_pending_bytes, 
        backlog,
        avg_latency_ms
        );
    // fflush(s->stats.f); 
  }
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

  if (s->db) {
    lsmt_flush(s->db);
  }

  if (s->stats.f) {
    fclose(s->stats.f);
    s->stats.f = NULL;
  }
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

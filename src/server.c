#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <raft/raft.h>
#include <raft/raft/uv.h>

#include <lsmt/lsmt.h>

#define N_SERVERS 3    /* Number of servers in the example cluster */
#define APPLY_RATE 125 /* Apply a new entry every 125 milliseconds */

#define Log(SERVER_ID, FORMAT) printf("%d: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
  printf("%d: " FORMAT "\n", SERVER_ID, __VA_ARGS__)


#define CONTENT_MAX_SIZE 255

const uint8_t CONTENT_HEADER_SIZE = sizeof(sl_uint128_t) + sizeof(uint8_t); 
const uint32_t INSERT_CMD_SIZE = CONTENT_HEADER_SIZE + sizeof(uint8_t)*CONTENT_MAX_SIZE; 

#ifdef __RUN_EXAMPLE
const bool RUN_EXAMPLE = true;
#else
const bool RUN_EXAMPLE = false;
#endif


typedef struct insert_cmd {
  uint32_t msg_size;
  sl_uint128_t record_key;
  uint8_t record_value[CONTENT_MAX_SIZE];
} insert_cmd_t;

lsmt_t *db;

struct Fsm
{
  insert_cmd_t insert_cmd;
};

/* buf struct:
 * 4 bytes: msg_length.
 * 16 bytes: record key.
 * (msg_length-4-16)bytes: record value.
 */
static int FsmApply(struct raft_fsm *fsm,
    const struct raft_buffer *buf,
    void **result)
{
  struct Fsm *f = fsm->data;

  /* Check if the buffer is at least the size of the header. */
  if (buf->len < sizeof(uint32_t) + sizeof(sl_uint128_t)) {
    return RAFT_MALFORMED;
  }
  const uint32_t msg_size = *(const uint32_t *)buf->base;

  if (buf->len < msg_size) {
    return RAFT_MALFORMED;
  }

  const uint32_t record_value_size = msg_size - sizeof(uint32_t) - sizeof(sl_uint128_t);

  if (record_value_size > CONTENT_MAX_SIZE) {
    return RAFT_MALFORMED;
  }

  const uint8_t *data = (const uint8_t *)buf->base; 

  memcpy(&f->insert_cmd.record_key,
      data + sizeof(uint32_t), sizeof(sl_uint128_t));

  memcpy(f->insert_cmd.record_value,
      data + sizeof(uint32_t) + sizeof(sl_uint128_t), record_value_size);

  uint32_t value_size = *((uint32_t *)(f->insert_cmd.record_value + sizeof(uint8_t)));

  /*
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
      f->insert_cmd.record_value, record_value_size);

  if (e != 0) {
    return RAFT_INVALID;
  }

  //f->count += *(uint64_t*)buf->base;
  //*result = &f->count;
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

static int FsmInit(struct raft_fsm *fsm)
{
  struct Fsm *f = raft_malloc(sizeof *f);
  if (f == NULL) {
    return RAFT_NOMEM;
  }
  memset(f, 0, sizeof(*f));
  //f->count = 0;
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



// --- NEW: Struct to hold the state for each connected client ---
typedef struct {
    uv_tcp_t handle;
    struct Server *server;
    uv_write_t write_req;

    // A dynamic buffer to handle the incoming TCP stream
    char *buffer;
    size_t buffer_len;
    size_t buffer_cap;
} client_t;

/********************************************************************
 *
 * Example struct holding a single raft server instance and all its
 * dependencies.
 *
 ********************************************************************/

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

struct Server
{
  lsmt_t *db;
  uv_tcp_t tcp_server_handle;
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
};

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

// --- NEW SECTION: All custom TCP logic goes here ---

// Forward declarations
static void on_client_close(uv_handle_t *handle);
static void process_buffer(client_t *client);

// Helper function to safely close and free a client's resources
static void close_and_free_client(client_t *client) {
    // The on_client_close callback will free the client memory
    uv_close((uv_handle_t *)&client->handle, on_client_close);
}

// Callback that fires after a client's handle is fully closed
static void on_client_close(uv_handle_t *handle) {
    client_t *client = (client_t *)handle->data;
    // Free all associated memory
    if (client->buffer) {
        free(client->buffer);
    }
    free(client);
    //Log(0, "TCP: Client disconnected and cleaned up.");
}

// Callback for after a write operation (e.g., sending a response) is complete
static void on_write_complete(uv_write_t *req, int status) {
    if (status) {
        fprintf(stderr, "Write error: %s\n", uv_strerror(status));
    }
    // You can free write-specific data here if you had any
}

// Libuv callback to allocate memory for an incoming client read
static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    buf->base = malloc(suggested_size);
    buf->len = suggested_size;
}

// This function is called every time we receive data from a client
static void on_client_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    client_t *client = (client_t *)stream->data;

    if (nread > 0) {
        // Append the new data to our client's buffer
        if (client->buffer_len + nread > client->buffer_cap) {
            client->buffer_cap = (client->buffer_len + nread) * 2;
            char *new_buf = realloc(client->buffer, client->buffer_cap);
            if (!new_buf) {
                // Out of memory
                free(buf->base);
                close_and_free_client(client);
                return;
            }
            client->buffer = new_buf;
        }
        memcpy(client->buffer + client->buffer_len, buf->base, nread);
        client->buffer_len += nread;

        // Try to process one or more complete messages from the buffer
        process_buffer(client);

    } else if (nread < 0) {
        if (nread != UV_EOF) {
            fprintf(stderr, "Read error: %s\n", uv_strerror(nread));
        }
        close_and_free_client(client);
    }

    // Libuv requires us to free the buffer from alloc_cb
    free(buf->base);
}

// This function implements the length-prefixed protocol parser
static void process_buffer(client_t *client) {
  struct Server *s = client->server;

  // Loop as long as we might have a complete message in the buffer
  while (1) {
    // A message needs at least a 4-byte length prefix
    if (client->buffer_len < sizeof(uint32_t)) {
      break; // Not enough data for even the length
    }

    // Read the length of the message payload
    uint32_t payload_len;
    memcpy(&payload_len, client->buffer, sizeof(uint32_t));

    uint32_t total_msg_len = payload_len;
    //size_t total_msg_len = sizeof(uint32_t) + payload_len;

    // Do we have the full message in our buffer?
    if (client->buffer_len < total_msg_len) {
      break; // Incomplete message, wait for more data
    }

    /* Extract the payload. */
    char *payload = client->buffer; 

    //Logf(s->id, "TCP: Received command with size %u", payload_len);

    if (s->raft.state != RAFT_LEADER) {
      Log(s->id, "TCP: Rejecting command, not the leader.");
      // In a real system, you'd send an error response here
    } else {
      // Copy the payload into a raft_buffer. We must copy it because
      // raft_apply takes ownership and our client buffer will be reused.
      struct raft_buffer raft_buf;
      uint32_t aligned_msg_size = (total_msg_len + 7) & ~0x07;

      raft_buf.len = aligned_msg_size; 
      raft_buf.base = raft_malloc(raft_buf.len); 
      if (!raft_buf.base) { 
        printf("Failed raft_malloc for incoming request in process_buffer\n");
        exit(1);
      }

      memcpy(raft_buf.base, payload, total_msg_len);
      struct raft_apply *req = raft_malloc(sizeof(*req));
      if (!req) { 
        printf("Failed raft_malloc for raft_apply request in process_buffer\n");
        exit(1);
        //raft_free(raft_buf.base);
      }

      /*
      printf("[PROCESS BUFFER] buffer (aligned) size=%lu, payload_len=%u\n",
          raft_buf.len, total_msg_len);
      fflush(stdout);
      */

      req->data = s;
      int rv = raft_apply(&s->raft, req, &raft_buf, 1, serverApplyCb);

      if (rv != 0) {
        Logf(s->id, "raft_apply() failed: %s", raft_errmsg(&s->raft));
        //raft_free(req);
        //raft_free(raft_buf.base); 
        return;
      } else {
        // Optionally send a success response to the client
        // uv_buf_t res_buf = ...;
        // uv_write(&client->write_req, (uv_stream_t*)&client->handle, &res_buf, 1, on_write_complete);
      }
    }

    // Remove the processed message from the buffer by shifting the remaining data
    if (client->buffer_len > total_msg_len) {
      memmove(client->buffer, client->buffer + total_msg_len, client->buffer_len - total_msg_len);
    }
    client->buffer_len -= total_msg_len;
  }
}

// Libuv callback for when a new client connects to our server
static void on_new_connection(uv_stream_t *server_handle, int status) {
    if (status < 0) {
        fprintf(stderr, "New connection error: %s\n", uv_strerror(status));
        return;
    }

    struct Server *s = (struct Server *)server_handle->data;
    
    // Allocate and initialize a new client struct
    client_t *client = calloc(1, sizeof(client_t));
    if (!client) { /* handle OOM */ return; }
    
    client->server = s;
    client->handle.data = client; // Important: back-pointer for callbacks
    client->write_req.data = client;

    uv_tcp_init(s->loop, &client->handle);

    if (uv_accept(server_handle, (uv_stream_t *)&client->handle) == 0) {
        //Log(s->id, "TCP: New client connected.");
        uv_read_start((uv_stream_t *)&client->handle, alloc_cb, on_client_read);
    } else {
        close_and_free_client(client);
    }
}

// --- END NEW SECTION ---




static int client_uv_init(struct Server *s, int port) {
  Log(s->id, "Setting up custom TCP server");

  uv_tcp_init(s->loop, &s->tcp_server_handle);
  s->tcp_server_handle.data = s; // Back-pointer to the server

  struct sockaddr_in addr;
  // Listen on port 7000 + server_id (e.g., 7001, 7002, 7003)
  uv_ip4_addr("0.0.0.0", port, &addr); 

  int rv = uv_tcp_bind(&s->tcp_server_handle, (const struct sockaddr*)&addr, 0);
  if (rv != 0) {
    Logf(s->id, "uv_tcp_bind(): %s", uv_strerror(rv));
    // Add proper cleanup here if other handles were init'd
    return -1;
    //goto err;
  }

  rv = uv_listen((uv_stream_t*)&s->tcp_server_handle, 128, on_new_connection);
  if (rv != 0) {
    Logf(s->id, "uv_listen(): %s", uv_strerror(rv));
    return -2;
    //goto err;
  }
  Logf(s->id, "Custom TCP server listening on port %d", port);
  return 0;
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
    bootstrap_node_t *bootstrap_node,
    unsigned id)
{
  struct raft_configuration configuration;
  struct timespec now;
  unsigned i;
  int rv;

  memset(s, 0, sizeof *s);

  /* Seed the random generator */
  timespec_get(&now, TIME_UTC);
  srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

  s->loop = loop;

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
  rv = FsmInit(&s->fsm);
  if (rv != 0) {
    Logf(s->id, "FsmInit(): %s", raft_strerror(rv));
    goto err_after_uv_init;
  }

  /* Save the server ID. */
  s->id = id;

  /* Render the address. */
  sprintf(s->address, "127.0.0.1:900%d", id);

  /* Initialize and start the engine, using the libuv-based I/O backend. */
  rv = raft_init(&s->raft, &s->io, &s->fsm, id, s->address);
  if (rv != 0) {
    Logf(s->id, "raft_init(): %s", raft_errmsg(&s->raft));
    goto err_after_fsm_init;
  }
  s->raft.data = s;

  /* Bootstrap the initial configuration if needed. */
  raft_configuration_init(&configuration);
  if (bootstrap_node != NULL && bootstrap_node->id >= 0) {
    char address[64];
    unsigned server_id = i + 1;
    sprintf(address, "127.0.0.1:900%d", server_id);
    rv = raft_configuration_add(&configuration, server_id, address,
        RAFT_VOTER);
    if (rv != 0) {
      Logf(s->id, "raft_configuration_add(): %s", raft_strerror(rv));
      goto err_after_configuration_init;
    }
  }
  for (i = 0; i < N_SERVERS; i++) {
    char address[64];
    unsigned server_id = i + 1;
    sprintf(address, "127.0.0.1:900%d", server_id);
    rv = raft_configuration_add(&configuration, server_id, address,
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

  s->transfer.data = s;

  /* Setup tcp connection for handling incoming requests. */
  rv = client_uv_init(s, 7000 + id);
  if (rv != 0) {
    exit(1);
    //goto err;
  }

  s->db = lsmt_init(dir);
  db = s->db;
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
  struct Server *s = req->data;
  //int count;
  raft_free(req);
  if (status != 0) {
    if (status != RAFT_LEADERSHIPLOST) {
      Logf(s->id, "raft_apply() callback: %s (%d)", raft_errmsg(&s->raft),
          status);
    }
    return;
  }
  /*
     count = *(int *)result;
     if (count % 100 == 0) {
     Logf(s->id, "count %d", count);
     }
     */
}

/* Called periodically every APPLY_RATE milliseconds. */
static void serverTimerCb(uv_timer_t *timer)
{
  struct Server *s = timer->data;
  struct raft_buffer buf;
  struct raft_apply *req;
  int rv;

  if (s->raft.state != RAFT_LEADER) {
    return;
  }
  uint32_t key_id = (rand() % (100000 << 1)) + 1;
  sl_uint128_t key = {
    .id = key_id,
    .timestamp = get_unix_epoch()
  };

  uint32_t record_value = key_id;

  /* Construct the value payload according to the format: [type][size][data]. */
  uint8_t value_payload[CONTENT_MAX_SIZE];
  uint8_t *p = value_payload; 

  /* Write the record type. */
  *p = LSMT_TYPE_INT;
  p += sizeof(uint8_t);

  /* Write the size of the record value. */
  uint32_t data_size = sizeof(record_value); 
  memcpy(p, &data_size, sizeof(data_size));
  p += sizeof(uint32_t);

  /* Write the record value itself. */
  memcpy(p, &record_value, data_size);
  p += data_size;

  uint32_t value_len = (uint32_t)(p - value_payload); 
  uint32_t total_msg_size = sizeof(uint32_t) + sizeof(sl_uint128_t) + value_len; 

  /* Entry buffers for raft_apply must be 8-byte aligned. */
  /* Every number multiple of 8 ends with three zeros.
   * With the & ~0x07 i'm erasing the last three bits,
   * rounding to the floor the number so i need to add +7. */
  uint32_t aligned_msg_size = (total_msg_size + 7) & ~0x07;

  buf.len = aligned_msg_size;
  buf.base = raft_malloc(buf.len);
  if (buf.base == NULL) {
    Log(s->id, "serverTimerCb(): out of memory");
    return;
  }

  printf("raft_apply buffer size=%lu\n", buf.len);
  fflush(stdout);

  uint8_t* buf_ptr = buf.base;

  /* Serialized the message size. */
  memcpy(buf_ptr, &total_msg_size, sizeof(uint32_t));
  buf_ptr += sizeof(uint32_t);

  /* Serialize the key. */
  memcpy(buf_ptr, &key, sizeof(key));
  buf_ptr += sizeof(key);

  /* Serialize the record value. */ 
  memcpy(buf_ptr, &value_payload, value_len); 

  req = raft_malloc(sizeof *req);
  if (req == NULL) {
    Log(s->id, "serverTimerCb(): out of memory");
    raft_free(buf.base);
    return;
  }

  req->data = s;

  rv = raft_apply(&s->raft, req, &buf, 1, serverApplyCb);
  if (rv != 0) {
    Logf(s->id, "raft_apply(): %s", raft_errmsg(&s->raft));
    raft_free(buf.base);
    raft_free(req);
    return;
  }
}

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

  if (RUN_EXAMPLE) {
    rv = uv_timer_start(&s->timer, serverTimerCb, 0, APPLY_RATE);
    if (rv != 0) {
      Logf(s->id, "uv_timer_start(): %s", uv_strerror(rv));
      goto err;
    }
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
  //lsmt_flush(s->db);
  //lsmt_free(s->db);

  /* Close the timer asynchronously if it was successfully
   * initialized. Otherwise invoke the callback immediately. */
  if (s->timer.data != NULL) {
    uv_close((struct uv_handle_s *)&s->timer, serverTimerCloseCb);
  } else {
    s->close_cb(s);
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
  struct uv_loop_s loop;
  struct uv_signal_s sigint; /* To catch SIGINT and exit. */
  struct Server server;
  const char *dir;
  unsigned id;
  int rv;

  printf("EXAMPLE=%d\n", RUN_EXAMPLE);

  if (argc != 3) {
    printf("usage: example-server <dir> <id>\n");
    return 1;
  }
  dir = argv[1];
  id = (unsigned)atoi(argv[2]);

  /* Ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254 */
  signal(SIGPIPE, SIG_IGN);

  /* Initialize the libuv loop. */
  rv = uv_loop_init(&loop);
  if (rv != 0) {
    Logf(id, "uv_loop_init(): %s", uv_strerror(rv));
    goto err;
  }

  /* Initialize the example server. */
  rv = ServerInit(&server, &loop, dir, NULL, id);
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

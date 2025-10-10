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
/********************************************************************
 *
 * Sample application FSM that just increases a counter.
 *
 ********************************************************************/
const uint8_t CONTENT_HEADER_SIZE = sizeof(sl_uint128_t) + sizeof(uint8_t); 
const uint32_t INSERT_CMD_SIZE = CONTENT_HEADER_SIZE + sizeof(uint8_t)*CONTENT_MAX_SIZE; 


typedef struct insert_cmd {
  sl_uint128_t key;
  uint8_t content_size;
  uint8_t encoded_data[CONTENT_MAX_SIZE];
} insert_cmd_t;
//
lsmt_t *db;

struct Fsm
{
  insert_cmd_t insert_cmd;
  //sl_uint128_t key;
  /* Includes 1 byte for the content type + 4 bytes for the content length. */
  //uint8_t content_size;
  //uint8_t encoded_data[CONTENT_MAX_SIZE];
  //unsigned long long count;
};

static int FsmApply(struct raft_fsm *fsm,
    const struct raft_buffer *buf,
    void **result)
{
  struct Fsm *f = fsm->data;
  if (buf->len != sizeof(insert_cmd_t)) {
    return RAFT_MALFORMED;
  }

  const uint8_t *data = (const uint8_t *)buf->base;
  memcpy(&f->insert_cmd.key, data, sizeof(sl_uint128_t));
  memcpy(&f->insert_cmd.content_size, data + sizeof(sl_uint128_t), sizeof(uint8_t));
  memcpy(f->insert_cmd.encoded_data, data + sizeof(sl_uint128_t) + sizeof(uint8_t), f->insert_cmd.content_size);

  /*
  insert_cmd_t req = f->insert_cmd;
  printf("insert: key=%lu %lu length=%d\n[", req.key.id, req.key.timestamp, req.content_size);
  fflush(stdout);
  for (int i = 0; i < req.content_size; i++) {
    printf(" %02x", req.encoded_data[i]);
    fflush(stdout);
  }
  printf("]\n\n");
  fflush(stdout);
  */
  int e = lsmt_insert(db, f->insert_cmd.key, f->insert_cmd.encoded_data, f->insert_cmd.content_size);
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

/* Initialize the example server struct, without starting it yet. */
static int ServerInit(struct Server *s,
    struct uv_loop_s *loop,
    const char *dir,
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

  raft_set_snapshot_threshold(&s->raft, 64);
  raft_set_snapshot_trailing(&s->raft, 16);
  raft_set_pre_vote(&s->raft, true);

  s->transfer.data = s;
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

  buf.len = sizeof(insert_cmd_t);
  //buf.len = sizeof(uint64_t);
  buf.base = raft_malloc(buf.len);
  if (buf.base == NULL) {
    Log(s->id, "serverTimerCb(): out of memory");
    return;
  }

  uint32_t key_id = (rand() % (100000 << 1)) + 1;
  sl_uint128_t key = {
    .id = key_id,
    .timestamp = get_unix_epoch()
  };


  /* 1. Construct the value payload according to the format: [type][size][data] */
  uint8_t value_payload[CONTENT_MAX_SIZE];
  uint8_t *p = value_payload; // Use a pointer to build the payload

  // The actual data we want to store is the key_id
  uint64_t data_to_store = key_id;
  uint32_t data_size = sizeof(data_to_store); // This is 4 bytes

  // a. Write the type (1 byte)
  *p = LSMT_TYPE_INT;
  p += sizeof(uint8_t);

  // b. Write the size of the *following data* (4 bytes)
  memcpy(p, &data_size, sizeof(uint32_t));
  p += sizeof(uint32_t);

  // c. Write the data itself (4 bytes)
  memcpy(p, &data_to_store, data_size);
  p += data_size;

  // The total size of the value payload we've constructed
  uint8_t total_payload_size = p - value_payload; // Correctly calculates to 9 bytes

  /* 2. Now, serialize the entire command into the raft buffer */
  uint8_t* buf_ptr = buf.base;

  // a. Serialize the key
  memcpy(buf_ptr, &key, sizeof(key));
  buf_ptr += sizeof(key);

  // b. Serialize the total_payload_size (which is the content_size for the command)
  memcpy(buf_ptr, &total_payload_size, sizeof(uint8_t));
  buf_ptr += sizeof(uint8_t);

  // c. Serialize the value payload itself. This fixes the sizeof() bug.
  memcpy(buf_ptr, value_payload, total_payload_size);



  /*
  uint8_t payload[CONTENT_MAX_SIZE];
  uint8_t payload_size = 0;

  payload[payload_size++] = LSMT_TYPE_INT;
  payload[payload_size] = sizeof(uint32_t);
  memcpy(payload + payload_size, &key_id, sizeof(uint32_t)); 
  payload_size += sizeof(uint8_t) + sizeof(uint32_t);

  // Copy the key
  memcpy(buf.base, &key, sizeof(key));
  
  // Copy the correct payload_size into the content_size field
  memcpy(buf.base + sizeof(key), &payload_size, sizeof(uint8_t));
  
  // Copy the actual payload into the encoded_data field
  memcpy(buf.base + sizeof(key) + sizeof(uint8_t), payload, payload_size);

  */
  req = raft_malloc(sizeof *req);
  if (req == NULL) {
    Log(s->id, "serverTimerCb(): out of memory");
    return;
  }
  req->data = s;

  rv = raft_apply(&s->raft, req, &buf, 1, serverApplyCb);
  if (rv != 0) {
    Logf(s->id, "raft_apply(): %s", raft_errmsg(&s->raft));
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
  rv = uv_timer_start(&s->timer, serverTimerCb, 0, 1);
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
  rv = ServerInit(&server, &loop, dir, id);
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

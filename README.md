# RAFT LSMT Distributed Key-Value Database

This project implements a distributed key-value database based on the Raft consensus protocol and Log-Structured Merge-Tree storage engine. 

---

## Prerequisites 
Ensure the following dependencies are installed on your system:

- `gcc`
- `make`
- `autoconf`, `automake`, `libtool` (required to compile the RAFT and libuv dependencies)
- `liblz4-dev` (LZ4 compression library development headers)
- `jq`: JSON processor, required for parsing cluster topologies in the scripts.

On Debian/Ubuntu-based systems, you can install these via:
```bash
sudo apt-get update
sudo apt-get install build-essential autoconf automake libtool liblz4-dev jq
```
---

## Building 
Fetches and builds library dependencies:
- custom LSM Tree;
- [libuv](https://github.com/libuv/libuv);
- [Raft](https://github.com/cowsql/raft).

```bash
make deps
```

Build executable:
```bash
make release
```

## Example Local Topology (`local-topology.json`)

To configure a cluster, define its topology in a JSON file.
Node IDs must be unique **per cluster**.

```json
{
  "A": [
    { "id": 1, "host": "127.0.0.1", "raft_port": 17001, "tcp_port": 18001 },
    { "id": 2, "host": "127.0.0.1", "raft_port": 17002, "tcp_port": 18002 },
    { "id": 3, "host": "127.0.0.1", "raft_port": 17003, "tcp_port": 18003 }
  ],
  "B": [
    { "id": 1, "host": "127.0.0.1", "raft_port": 17004, "tcp_port": 18004 },
    { "id": 2, "host": "127.0.0.1", "raft_port": 17005, "tcp_port": 18005 },
    { "id": 3, "host": "127.0.0.1", "raft_port": 17006, "tcp_port": 18006 }
  ]
}
```

---

## Running and Managing Clusters

### 1. Setup Clusters
Configure the directory structure on each machine. creates folders only for nodes mapping to the current address machine.
```bash
./setup-clusters.sh <topology_json> -o <output_dir>
```

### 2. Start Clusters
```bash
./run-clusters.sh -s <output_dir> [-c <cluster_name>] [-n <node_id>]
```

Run all local nodes in all clusters:
```bash
./run-clusters.sh -s my-clusters
```

Run all local nodes in cluster A:
```bash
./run-clusters.sh -s my-clusters -c A
```

Run only node 2 of cluster B:
```bash
./run-clusters.sh -s my-clusters -c B -n 2
```

### 3. Stop Clusters
```bash
./stop-clusters.sh -s <output_dir> [-c <cluster_name>] [-n <node_id>]
```

Stop all local nodes:
```bash
./stop-clusters.sh -s my-clusters
```

Stop all local nodes in cluster A:
```bash
./stop-clusters.sh -s my-clusters -c A
```

Stop only node 2 of cluster B:
```bash
./stop-clusters.sh -s my-clusters -c B -n 2
```

## Client Python CLI
connect to the cluster topology and perform manual write or query operations.

```bash
python3 cli.py --config <topology_json> [--routing-strategy {hash,round-robin,range}] [--max-key <max_key>]
```

```bash
python3 cli.py --config local-topology.json --routing-strategy range --max-key 5000000
```

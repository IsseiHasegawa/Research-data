# Distributed Key-Value Store Research Prototype

A lightweight distributed key-value store designed for experimentally studying trade-offs between failure detection settings and replication modes. Built for a junior seminar research project.

> **This is a research artifact, not a production system.** It is designed for reproducibility, measurability, and simplicity.

## Architecture Overview

```
┌─────────────────────┐     heartbeat + replication       ┌─────────────────────┐
│                     │ ◄──────────── TCP ──────────────► │                     │
│  Node 0 (Primary)   │                                   │  Node 1 (Secondary) │
│  - Accepts SET/GET  │                                   │  - Receives replicas│
│  - Replicates writes│                                   │  - Responds to HB   │
│  - Monitors Node 1  │                                   │  - Stores local KV  │
│  Port: 9100         │                                   │  Port: 9101         │
└─────────────────────┘                                   └─────────────────────┘
         ▲
         │ SET/GET (TCP)
         │
┌─────────────────────┐
│  Workload Generator │
│  (kv_workload)      │
└─────────────────────┘
```

**Key components:**

| Component | Language | Purpose |
|-----------|----------|---------|
| `kvnode` | C++17 | Node process (KV store, heartbeat, replication) |
| `kv_workload` | C++17 | Workload generator (SET/GET client) |
| `run_experiments.py` | Python 3 | Experiment orchestration |
| `compute_metrics.py` | Python 3 | Metrics computation |
| `plot_results.py` | Python 3 | Visualization |

## Build Instructions

### Prerequisites

- C++17 compiler (GCC 7+, Clang 5+, Apple Clang)
- CMake 3.10+
- Python 3.10+ with packages: `pandas`, `numpy`, `matplotlib`, `seaborn`

### Build

```bash
cd new-kv-store
mkdir -p build && cd build
cmake ..
make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu)
```

This produces two binaries in `build/`:
- `kvnode` — the node process
- `kv_workload` — the workload generator

### Install Python Dependencies

```bash
pip install -r requirements.txt
```

## Running Manually

### 1. Start the secondary node (Node 1)

```bash
./build/kvnode \
  --id node1 --port 9101 \
  --log_path output/logs/manual/node1.jsonl \
  --hb_interval_ms 100 --hb_timeout_ms 400
```

### 2. Start the primary node (Node 0)

```bash
./build/kvnode \
  --id node0 --port 9100 --primary \
  --peer 127.0.0.1:9101 \
  --log_path output/logs/manual/node0.jsonl \
  --hb_interval_ms 100 --hb_timeout_ms 400 \
  --repl_mode sync
```

### 3. Run the workload generator

```bash
./build/kv_workload \
  --target 127.0.0.1:9100 \
  --num_ops 100 --set_ratio 0.5 --rate 50 \
  --log_path output/logs/manual/workload.jsonl
```

### 4. Inject a fault (kill Node 1)

```bash
# In another terminal:
kill -9 $(pgrep -f "kvnode.*node1")
```

### 5. Observe

Check `node0.jsonl` for a `declared_dead` event after ~`hb_timeout_ms` milliseconds.

## Running Automated Experiments

### Full experiment sweep (all 8 configurations × 5 trials)

```bash
python3 scripts/run_experiments.py
```

### Run a subset

```bash
# Only configurations matching "sync"
python3 scripts/run_experiments.py --filter sync --trials 3

# Custom config file
python3 scripts/run_experiments.py --config config/experiment_configs.json --trials 10
```

### Analysis only (after experiments)

```bash
python3 scripts/compute_metrics.py --output output
python3 scripts/plot_results.py --output output
```

## CLI Reference

### `kvnode`

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--id <name>` | Yes | — | Node identifier |
| `--port <port>` | Yes | — | TCP listen port |
| `--log_path <path>` | Yes | — | JSONL log output |
| `--primary` | No | false | Marks node as primary |
| `--peer <host:port>` | If primary | — | Peer address for heartbeat + replication |
| `--hb_interval_ms <ms>` | No | 100 | Heartbeat send interval |
| `--hb_timeout_ms <ms>` | No | 400 | Timeout before declaring peer dead |
| `--repl_mode <mode>` | No | none | `none`, `sync`, or `async` |
| `--run_id <id>` | No | `$RUN_ID` or `default_run` | Experiment run ID |

### `kv_workload`

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--target <host:port>` | Yes | — | Target node address |
| `--log_path <path>` | Yes | — | JSONL log output |
| `--num_ops <n>` | No | 100 | Number of operations |
| `--set_ratio <0-1>` | No | 0.5 | Fraction of SET ops |
| `--rate <ops/sec>` | No | 0 (unlimited) | Target ops per second |
| `--key_space <n>` | No | 50 | Number of distinct keys |
| `--run_id <id>` | No | `$RUN_ID` | Experiment run ID |

## Log Format

All logs use **JSONL** (newline-delimited JSON). Each line is a self-contained event.

### Node logs (`node*.jsonl`)

```json
{"ts_ms":1711900000123,"node_id":"node0","run_id":"exp_001","event":"hb_ping_sent","peer_id":"peer","extra":{}}
{"ts_ms":1711900000150,"node_id":"node0","run_id":"exp_001","event":"hb_ack_recv","peer_id":"peer","extra":{}}
{"ts_ms":1711900001520,"node_id":"node0","run_id":"exp_001","event":"declared_dead","peer_id":"peer","extra":{}}
{"ts_ms":1711900000200,"node_id":"node0","run_id":"exp_001","event":"client_req_recv","peer_id":null,"extra":{"op":"SET","key":"key_0","req_id":"r_0"}}
{"ts_ms":1711900000201,"node_id":"node0","run_id":"exp_001","event":"repl_start","peer_id":"peer","extra":{"key":"key_0","seq":0,"mode":"sync"}}
{"ts_ms":1711900000205,"node_id":"node0","run_id":"exp_001","event":"repl_ack","peer_id":"peer","extra":{"seq":0,"mode":"sync"}}
{"ts_ms":1711900000206,"node_id":"node0","run_id":"exp_001","event":"client_req_done","peer_id":null,"extra":{"op":"SET","key":"key_0","req_id":"r_0","repl_ok":true}}
```

### Event types

| Event | Description |
|-------|-------------|
| `node_start` | Node process started |
| `node_stop` | Node process stopping |
| `hb_ping_sent` | Heartbeat ping sent to peer |
| `hb_ack_recv` | Heartbeat ack received from peer |
| `declared_dead` | Peer declared failed |
| `client_req_recv` | Client request received |
| `client_req_done` | Client request completed |
| `repl_start` | Replication to secondary started |
| `repl_ack` | Replication acknowledged |
| `repl_sent_async` | Async replication sent (no wait) |
| `repl_skipped` | Replication skipped (peer dead) |
| `fault_injected` | Artificial delay activated |

### Workload logs (`workload.jsonl`)

```json
{"ts_ms":1711900000300,"run_id":"exp_001","event":"op_start","op_type":"SET","key":"key_5","req_id":"r_0","success":true}
{"ts_ms":1711900000305,"run_id":"exp_001","event":"op_done","op_type":"SET","key":"key_5","req_id":"r_0","latency_us":5200,"success":true}
```

## Metrics

| Metric | Definition | How computed |
|--------|------------|--------------|
| Detection latency | Time from fault to `declared_dead` | `t_detect - t_fault` (from injector + node0 logs) |
| Downtime | Time from fault until first successful op | First successful workload op after `t_fault` |
| Write latency | Per-op round-trip time | From workload `op_done` events (median, p95, p99) |
| Throughput | Successful ops per second | `success_ops / duration` |
| False positives | Dead declarations before any fault | `declared_dead` events before `fault_inject` |
| Data loss risk | Writes not replicated before crash | Count of `repl_skipped` events |

### Output CSVs

- `output/results/summary.csv` — Per-trial metrics
- `output/results/aggregate.csv` — Per-config aggregated (median, IQR)
- `output/results/heatmap.csv` — For heatmap visualization
- `output/results/scatter.csv` — For scatter plot visualization

## Experiment Configurations

See `config/experiment_configs.json` for the 8 default configurations:

| Config | HB Interval | HB Timeout | Replication | Fault |
|--------|-------------|------------|-------------|-------|
| `fd_fast_no_repl` | 50ms | 150ms | none | crash |
| `fd_slow_no_repl` | 200ms | 600ms | none | crash |
| `fd_fast_sync_repl` | 50ms | 150ms | sync | crash |
| `fd_fast_async_repl` | 50ms | 150ms | async | crash |
| `fd_slow_sync_repl` | 200ms | 600ms | sync | crash |
| `fd_slow_async_repl` | 200ms | 600ms | async | crash |
| `delay_fast_sync` | 50ms | 150ms | sync | delay |
| `delay_slow_async` | 200ms | 600ms | async | delay |

## Design Decisions

### Why raw TCP sockets?
Simplicity and inspectability. No external dependencies to install or debug. The JSON-over-TCP protocol is easy to trace with standard tools.

### Why wall clock for log timestamps?
All processes run on one machine. Wall clock alignment is sufficient (< 1ms skew). This avoids the complexity of monotonic-to-wall clock conversion across processes.

### Why monotonic clock for internal timeouts?
Monotonic clock is immune to NTP adjustments, ensuring the failure detector's timeout logic is reliable regardless of system clock changes.

### Why application-level delay injection?
macOS does not support Linux `tc` (traffic control). Application-level delay simulation via a control message is portable, simpler, and sufficient for measuring the impact of delays on failure detection. **Note**: This simulates processing delay, not true network-layer packet delay.

### Why separate connections for heartbeat and replication?
Prevents replication traffic from interfering with heartbeat timing. If replication blocks on a slow write, it should not delay heartbeat responses.

## Extending to 3+ Nodes

The code is structured to support extension:

1. **Multiple peers**: Change `--peer` to accept a comma-separated list. Create one `FailureDetector` per peer.
2. **Multi-target replication**: Modify `Replicator` to maintain connections to multiple secondaries. For sync mode, wait for all (or a quorum of) acks.
3. **Flexible primary selection**: Add a `--role` flag or simple leader election.
4. **Updated config**: Add `num_nodes` and per-node port lists to `experiment_configs.json`.

Key code changes needed:
- `node.hpp`: Loop over peer list, create FD + replicator per peer
- `replicator.hpp`: Vector of peer connections, quorum logic for sync
- `run_experiments.py`: Start N node processes, configure peer mesh

## Limitations and Threats to Validity

1. **Single machine**: No real network latency or partitions. All communication goes through the loopback interface.
2. **Application-level delay**: Not equivalent to true network delay. Delay is applied after message receipt, not during transmission.
3. **OS scheduling jitter**: Detection latency depends on thread scheduling. Results may vary under system load. Mitigated by running multiple trials and reporting IQR.
4. **Simple primary/secondary**: No leader election, no automatic failover. Primary must be pre-designated.
5. **No persistence**: All data is in-memory. Node restart loses all state.
6. **TCP connection model**: One connection per client/peer. No connection pooling or multiplexing.
7. **Clock precision**: Wall clock timestamps have ~1ms granularity. Sub-millisecond effects are not measurable.
8. **Thread-per-connection**: Adequate for the small scale of this prototype but would not scale to many concurrent clients.

## Project Structure

```
new-kv-store/
├── CMakeLists.txt              # Build system
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── src/
│   ├── common/
│   │   ├── message.hpp         # Wire protocol: message types + JSON serialization
│   │   ├── logger.hpp          # Thread-safe JSONL structured logger
│   │   └── net.hpp             # TCP socket utilities
│   ├── node/
│   │   ├── kv_store.hpp        # In-memory key-value store
│   │   └── node.hpp            # Main node logic (server, dispatch, handlers)
│   ├── failure_detector/
│   │   └── heartbeat.hpp       # Heartbeat sender/receiver/checker
│   ├── replication/
│   │   └── replicator.hpp      # Sync/async replication logic
│   ├── client/
│   │   └── workload.cpp        # Workload generator binary
│   └── main.cpp                # Node entry point
├── scripts/
│   ├── run_experiments.py      # Experiment orchestration
│   ├── parse_logs.py           # Log parsing utilities
│   ├── compute_metrics.py      # Metrics computation
│   └── plot_results.py         # Visualization
├── config/
│   └── experiment_configs.json # Experiment parameter configurations
└── output/                     # Created at runtime
    ├── logs/                   # Per-trial JSONL logs
    ├── results/                # CSV metrics
    └── plots/                  # PNG visualizations
```

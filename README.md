# Raft Distributed Key-Value Store

![CI Status](https://github.com/aleciancia/raft-kv-store/actions/workflows/ci.yml/badge.svg)

A fault-tolerant, distributed Key-Value store built from scratch in pure Python. It implements the **Raft Consensus Algorithm** to ensure data integrity, high availability, and leader election across isolated network partitions. 

This project was built without any external dependencies to deeply explore distributed systems concepts, asynchronous network programming, and low-level state machines.

## Architecture & Features

* **Raft Leader Election:** Randomized election timers and automated state transitions (Follower -> Candidate -> Leader).
* **Log Replication:** Two-Phase Commit architecture over UDP heartbeats to ensure consensus before committing data.
* **Write-Ahead Log (WAL) Persistence:** Disk-based event sourcing using `os.fsync` for crash recovery and state reconstruction.
* **Asynchronous Networking:** Non-blocking `asyncio` Datagram endpoints for concurrent multi-node communication.
* **Transparent Client Routing:** Followers automatically redirect client requests to the active Leader.
* **Zero Dependencies:** Built entirely with the Python standard library.

## Quick Start

### 1. Start the Cluster
Open three separate terminal windows to simulate a 3-node local cluster.

**Terminal 1:**
```bash
python3 node.py --id node_1 --port 5001
```

**Terminal 2:**
```bash
python3 node.py --id node_2 --port 5002
```

**Terminal 3:**
```bash
python3 node.py --id node_3 --port 5003
```

Watch the terminals as the nodes automatically negotiate and elect a Leader.

### 2. Interact with the Database
Open a 4th terminal to act as the client. The client will automatically discover the Leader even if you point it to a Follower node.

**Write a value (SET):**
```bash
python3 client.py SET tech_stack Python
```

**Read a value (GET):**
```bash
python3 client.py GET tech_stack
```

**Delete a value (DELETE):**
```bash
python3 client.py DELETE tech_stack
```

### 3. Simulate Node Failures
To test fault tolerance, stop the Leader's process (`Ctrl+C`). The remaining two nodes will detect the missing heartbeats and immediately elect a new Leader. All previously committed data will remain safe and available.

## Testing

The project includes an asynchronous test suite utilizing `unittest.mock` to simulate network isolated environments and verify state machine transitions.

```bash
python3 -m unittest discover tests/
```
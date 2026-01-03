# Mini-Coordinator

A failure-aware distributed job orchestrator and Parameter Server (PS) built from scratch. This project demonstrates the core patterns used in systems like **Kubernetes**, **Ray**, and **PyTorch Distributed**.

## 🚀 Key Features

- **Failure Detection:** Background "reaper" loops detect dead workers via heartbeat timeouts (Soft-state membership).
- **Concurrency-Safe Orchestration:** Centralized coordinator manages a FIFO job queue with a thread-safe in-memory registry.
- **Distributed Fencing:** Prevents "Split-Brain" errors using monotonic attempt tokens and time-based leases.
- **AI Parameter Server:** Implements `PullWeights` and `PushGradients` for distributed Stochastic Gradient Descent (SGD).
- **Self-Fencing Workers:** Workers use Go `context` timeouts to self-terminate tasks if their lease expires.

## 🏗️ Architecture

- **Coordinator (Go):** Manages the job queue, worker registry, and global model weights.
- **Workers (Go):** Poll for work, execute AI workloads (synthetic linear regression), and report results.
- **gRPC/Protobuf:** High-performance, binary-encoded communication protocol.

## 🛠️ Tech Stack

- **Language:** Go (Golang)
- **Communication:** gRPC & Protocol Buffers
- **Logic:** Mutual Exclusion (Mutexes), Goroutines, Channels
- **Workload:** Distributed AI Training (Linear Regression)

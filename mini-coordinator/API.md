# API Reference

This document provides a reference for the gRPC services defined in the **Mini-Coordinator** project.

## Table of Contents

- [Coordinator Service](#coordinator-service)
  - [RegisterWorker](#registerworker)
  - [Heartbeat](#heartbeat)
  - [PollWork](#pollwork)
  - [ReportWork](#reportwork)
  - [SubmitJob](#submitjob)
  - [GetStatus](#getstatus)
- [Parameter Server Service](#parameter-server-service)
  - [InitModel](#initmodel)
  - [PullWeights](#pullweights)
  - [PushGradients](#pushgradients)
  - [GetPSStatus](#getpsstatus)

---

## Coordinator Service

The **Coordinator** is the central brain of the distributed system. It manages worker memberships, job queues, and fault tolerance.

### `RegisterWorker`

Allows a new worker node to join the cluster. The coordinator assigns a unique `worker_id` which must be used in subsequent calls.

- **Request:** `RegisterWorkerRequest`
  - `hostname` (string): The network address or hostname of the worker.
  - `labels` (map<string, string>): Metadata tags (e.g., `gpu=true`).
- **Response:** `RegisterWorkerResponse`
  - `worker_id` (string): The assigned unique ID.
  - `heartbeat_interval_ms` (int64): How often the worker should send heartbeats.

### `Heartbeat`

Signals liveness. Workers must call this periodically. If a heartbeat is missed for a configurable duration (default 6s), the coordinator marks the worker as dead.

- **Request:** `HeartbeatRequest`
  - `worker_id` (string): The ID of the worker.
- **Response:** `HeartbeatResponse`
  - `server_time_unix_ms` (int64): Coordinator's clock time, used for drift estimation.

### `PollWork`

Workers call this to request a job. The coordinator implements a FIFO queue.

- **Request:** `PollWorkRequest`
  - `worker_id` (string): The ID of the requesting worker.
- **Response:** `PollWorkResponse`
  - **OneOf Result:**
    - `job` (`AssignedJob`): Details of the assigned job.
      - `job_id`: Unique job ID.
      - `payload`: Job data.
      - `lease_duration_ms`: Time before the job is reclaimed.
      - `attempt`: Attempt counter (starts at 1).
    - `no_job` (`NoJob`): Instruction to retry later.
      - `retry_after_ms`: Backoff duration.

### `ReportWork`

Used by workers to update the status of an assigned job (Success, Failure, or progress update).

- **Request:** `ReportWorkRequest`
  - `worker_id` (string): The ID of the worker.
  - `job_id` (string): The job being updated.
  - `status` (`WorkStatus`): `SUCCEEDED`, `FAILED`, or `UNSPECIFIED`.
  - `attempt` (int32): The attempt number the worker is reporting on (must match server).
- **Response:** `ReportWorkResponse` (Empty)

### `SubmitJob`

Allows external clients (or workers) to add a new job to the queue.

- **Request:** `SubmitJobRequest`
  - `payload` (string): The job data/command.
- **Response:** `SubmitJobResponse`
  - `job_id` (string): The ID of the newly created job.

### `GetStatus`

Returns the global state of the cluster.

- **Request:** `GetStatusRequest` (Empty)
- **Response:** `GetStatusResponse`
  - `workers` (list): List of registered workers and their heartbeat status.
  - `jobs` (list): List of jobs and their states (`PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `REQUEUED`).

---

## Parameter Server Service

The **Parameter Server (PS)** manages shared state for distributed machine learning training (specifically Linear Regression in this example).

### `InitModel`

Initializes a model with a starting weight vector.

- **Request:** `InitModelRequest`
  - `model_id` (string): Unique identifier for the model.
  - `weights` (list<float>): Initial weights.
- **Response:** `InitModelResponse`
  - `version` (int64): Initial version number (1).

### `PullWeights`

Retrieves the current weights of a model.

- **Request:** `PullWeightsRequest`
  - `model_id` (string): The model to retrieve.
  - `min_version` (int64, optional): If provided, the server only returns weights if the current version > `min_version`.
- **Response:** `PullWeightsResponse`
  - `weights` (list<float>): The weight vector.
  - `version` (int64): The current version number.

### `PushGradients`

Applies gradients to update the model weights using Stochastic Gradient Descent (SGD).

`NewWeights = OldWeights - (LearningRate * Gradients)`

- **Request:** `PushGradientsRequest`
  - `model_id` (string): The model to update.
  - `gradients` (list<float>): The calculated gradients.
  - `learning_rate` (float): The step size.
- **Response:** `PushGradientsResponse`
  - `new_version` (int64): The version number after the update.

### `GetPSStatus`

Returns metadata about all models hosted on the server.

- **Request:** `GetPSStatusRequest` (Empty)
- **Response:** `GetPSStatusResponse`
  - `models` (list): List of models, their dimensions, and versions.

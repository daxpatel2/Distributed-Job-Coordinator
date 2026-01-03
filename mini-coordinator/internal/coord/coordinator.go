package coord

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	coordv1 "example.com/mini-coordinator/mini-coordinator/gen/example.com/mini-coordinator/gen/coordv1"
)

const (
	workerTimeout  = 6 * time.Second
	reaperInterval = 1 * time.Second
)

// JobState represents the current state of a job in its lifecycle.
type JobState int

const (
	JobPending   JobState = iota // Job is waiting in the queue.
	JobRunning                   // Job has been assigned to a worker and is executing.
	JobSucceeded                 // Job completed successfully.
	JobFailed                    // Job failed permanently.
	JobRequeued                  // Job failed or timed out and was added back to the queue.
)

// Job represents a unit of work to be executed by a worker.
// It tracks the payload, current state, assignment, and retry history.
type Job struct {
	ID      string
	Payload string
	State   JobState
	// AssignedWorker is the ID of the worker currently processing this job. Empty if not assigned.
	AssignedWorker string
	// LeaseExpiresAt is the deadline for the worker to report status.
	// The Coordinator says, "You have this job for 10 seconds. If I don't hear from you by then, I'm taking it back and giving it to someone else."
	LeaseExpiresAt time.Time
	LastMessage    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	// Attempts tracks how many times this job has been assigned. Used for fencing and retry limits.
	Attempts int
}

// Worker represents a connected compute node in the distributed system.
// It holds metadata and liveness information.
type Worker struct {
	ID            string
	Hostname      string
	Labels        map[string]string
	LastHeartbeat time.Time
	RegisteredAt  time.Time
}

// Coordinator is the central authority of the distributed system.
// It manages the registry of workers, the lifecycle of jobs, and fault tolerance mechanisms.
type Coordinator struct {
	coordv1.UnimplementedCoordinatorServer
	mu sync.Mutex

	workers map[string]*Worker
	jobs    map[string]*Job
	queue   []string // Job IDs in FIFO order

	heartbeatInterval time.Duration
	leaseDuration     time.Duration
}

// NewCoordinator initializes and returns a new Coordinator instance with default configuration.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers:           make(map[string]*Worker),
		jobs:              make(map[string]*Job),
		queue:             make([]string, 0),
		heartbeatInterval: 2 * time.Second,
		leaseDuration:     10 * time.Second,
	}
}

// RegisterWorker handles the initial registration of a worker node.
// It assigns a unique ID and records the worker's presence.
//
// Context: gRPC request context.
// Request: Contains worker metadata like hostname.
// Returns: A response containing the assigned WorkerID and heartbeat configuration.
func (c *Coordinator) RegisterWorker(ctx context.Context, req *coordv1.RegisterWorkerRequest) (*coordv1.RegisterWorkerResponse, error) {
	if req.GetHostname() == "" {
		return nil, fmt.Errorf("hostname is required")
	}

	workerID := "w_" + randHex(6)
	now := time.Now()

	w := &Worker{
		ID:            workerID,
		Hostname:      req.GetHostname(),
		Labels:        req.GetLabels(),
		LastHeartbeat: now,
		RegisteredAt:  now,
	}

	c.mu.Lock()
	c.workers[workerID] = w
	hb := c.heartbeatInterval
	c.mu.Unlock()

	return &coordv1.RegisterWorkerResponse{
		WorkerId:            workerID,
		HeartbeatIntervalMs: hb.Milliseconds(),
	}, nil
}

// Heartbeat processes a liveness signal from a worker.
// It updates the LastHeartbeat timestamp for the worker, preventing it from being reaped.
// Returns the current server time to help with clock drift estimation (though not strictly NTP).
func (c *Coordinator) Heartbeat(ctx context.Context, req *coordv1.HeartbeatRequest) (*coordv1.HeartbeatResponse, error) {
	now := time.Now()
	c.mu.Lock()
	w, ok := c.workers[req.GetWorkerId()]

	if ok {
		w.LastHeartbeat = now
	}
	c.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("unknown worker_id: %s", req.GetWorkerId())
	}

	return &coordv1.HeartbeatResponse{
		ServerTimeUnixMs: now.UnixMilli(),
	}, nil
}

// GetStatus returns a snapshot of the cluster state, including all workers and jobs.
// This is primarily used for debugging and monitoring.
func (c *Coordinator) GetStatus(ctx context.Context, req *coordv1.GetStatusRequest) (*coordv1.GetStatusResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := &coordv1.GetStatusResponse{}

	for _, w := range c.workers {
		out.Workers = append(out.Workers, &coordv1.WorkerInfo{
			WorkerId:            w.ID,
			Hostname:            w.Hostname,
			LastHeartbeatUnixMs: w.LastHeartbeat.UnixMilli(),
		})
	}

	for _, j := range c.jobs {
		out.Jobs = append(out.Jobs, &coordv1.JobInfo{
			JobId:              j.ID,
			Payload:            j.Payload,
			State:              toProtoJobState(j.State),
			AssignedWorkerId:   j.AssignedWorker,
			LeaseExpiresUnixMs: j.LeaseExpiresAt.UnixMilli(),
			LastMessage:        j.LastMessage,
		})
	}

	return out, nil
}

func toProtoJobState(s JobState) coordv1.JobState {
	switch s {
	case JobPending:
		return coordv1.JobState_JOB_STATE_PENDING
	case JobRunning:
		return coordv1.JobState_JOB_STATE_RUNNING
	case JobSucceeded:
		return coordv1.JobState_JOB_STATE_SUCCEEDED
	case JobFailed:
		return coordv1.JobState_JOB_STATE_FAILED
	case JobRequeued:
		return coordv1.JobState_JOB_STATE_REQUEUED
	default:
		return coordv1.JobState_JOB_STATE_UNSPECIFIED
	}
}

// SubmitJob accepts a new job payload from a client and adds it to the queue.
// The job starts in the JobPending state.
func (c *Coordinator) SubmitJob(ctx context.Context, req *coordv1.SubmitJobRequest) (*coordv1.SubmitJobResponse, error) {
	if req.GetPayload() == "" {
		return nil, fmt.Errorf("payload is required")
	}

	jobID := "j_" + randHex(6)
	now := time.Now()
	j := &Job{
		ID:        jobID,
		Payload:   req.GetPayload(),
		State:     JobPending,
		CreatedAt: now,
		UpdatedAt: now,
		Attempts:  0,
	}
	c.mu.Lock()
	c.jobs[jobID] = j
	c.queue = append(c.queue, jobID)
	c.mu.Unlock()

	return &coordv1.SubmitJobResponse{JobId: jobID}, nil
}

// PollWork checks the job queue for available work and assigns it to the requesting worker.
// It enforces FIFO ordering and lease management.
func (c *Coordinator) PollWork(ctx context.Context, req *coordv1.PollWorkRequest) (*coordv1.PollWorkResponse, error) {
	workerID := req.GetWorkerId()
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure worker exists
	if _, ok := c.workers[workerID]; !ok {
		return nil, fmt.Errorf("unknown worker_id: %s", workerID)
	}

	// Iterate through the queue to find the first assignable job.
	// We might encounter job IDs that were already processed (if we didn't clean up queue perfectly)
	// or jobs that aren't in a pending state.
	for len(c.queue) > 0 {
		jobID := c.queue[0]
		c.queue = c.queue[1:]
		j, ok := c.jobs[jobID]
		if !ok {
			// Job might have been deleted?
			continue
		}
		// find a pending job
		if j.State != JobPending && j.State != JobRequeued {
			continue
		}

		// found a pending job, set its parameters and assign to worker
		j.State = JobRunning
		j.AssignedWorker = workerID
		j.Attempts++
		// Lease logic: The worker has exclusive rights to this job for c.leaseDuration.
		// If they don't report back, the reaper will take it away.
		j.LeaseExpiresAt = now.Add(c.leaseDuration)
		j.UpdatedAt = now
		j.LastMessage = "assigned"

		return &coordv1.PollWorkResponse{
			Result: &coordv1.PollWorkResponse_Job{
				Job: &coordv1.AssignedJob{
					JobId:           j.ID,
					Payload:         j.Payload,
					LeaseDurationMs: c.leaseDuration.Milliseconds(),
					Attempt:         int32(j.Attempts),
				},
			},
		}, nil
	}

	// No work available, tell worker to back off.
	return &coordv1.PollWorkResponse{
		Result: &coordv1.PollWorkResponse_NoJob{
			NoJob: &coordv1.NoJob{RetryAfterMs: 500},
		},
	}, nil
}

// ReportWork updates the status of a job based on a worker's report.
// It handles success, failure, and fencing (ensuring only the valid owner can report).
func (c *Coordinator) ReportWork(ctx context.Context, req *coordv1.ReportWorkRequest) (*coordv1.ReportWorkResponse, error) {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[req.GetJobId()]
	if !ok {
		return nil, fmt.Errorf("unknown job_id: %s", req.GetJobId())
	}

	// Fencing: Ensure the reporting worker is the current owner and is working on the current attempt.
	// This prevents a "zombie" worker (one that was thought dead but came back) from overwriting
	// the work of a new worker assigned to the same job.
	if int(req.GetAttempt()) != j.Attempts {
		return nil, fmt.Errorf("stale report: job=%s got_attempt=%d current_attempt=%d",
			j.ID, req.GetAttempt(), j.Attempts)
	}
	if j.AssignedWorker != req.GetWorkerId() {
		return nil, fmt.Errorf("wrong owner: job=%s assigned_to=%s reporter=%s",
			j.ID, j.AssignedWorker, req.GetWorkerId())
	}

	j.UpdatedAt = now
	j.LastMessage = req.GetMessage()

	switch req.GetStatus() {
	case coordv1.WorkStatus_WORK_STATUS_SUCCEEDED:
		j.State = JobSucceeded
		j.LeaseExpiresAt = time.Time{}
	case coordv1.WorkStatus_WORK_STATUS_FAILED:
		// simple policy: requeue once, otherwise mark failed (we’ll tune later)
		if j.Attempts < 2 {
			j.State = JobPending
			j.AssignedWorker = ""
			j.LeaseExpiresAt = time.Time{}
			c.queue = append(c.queue, j.ID)
			j.LastMessage = "failed -> requeued: " + j.LastMessage
		} else {
			j.State = JobFailed
			j.LeaseExpiresAt = time.Time{}
			j.LastMessage = "failed permanently: " + j.LastMessage
		}
	default:
		return nil, fmt.Errorf("invalid status")
	}
	return &coordv1.ReportWorkResponse{}, nil
}

// RunReaper starts a background goroutine that periodically checks for:
// 1. Dead workers (missed heartbeats).
// 2. Expired job leases.
// It should be run in a separate goroutine.
func (c *Coordinator) RunReaper(stopCh <-chan struct{}) {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.reapDeadWorkers()
		case <-stopCh:
			return
		}
	}
}

// reapDeadWorkers implements the failure detection and recovery logic.
// It scans the worker registry and job list to enforce soft-state membership and lease guarantees.
func (c *Coordinator) reapDeadWorkers() {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Detect dead workers
	// If a worker hasn't sent a heartbeat within workerTimeout, we assume it crashed or is partitioned.
	dead := make(map[string]struct{})
	for id, w := range c.workers {
		if now.Sub(w.LastHeartbeat) > workerTimeout {
			fmt.Printf("worker %s timed out (last hb %v ago)\n", id, now.Sub(w.LastHeartbeat))
			delete(c.workers, id)
			dead[id] = struct{}{}
		}
	}

	// 2. Reclaim jobs
	// We look for jobs that need to be re-assigned.
	for _, j := range c.jobs {
		if j.State != JobRunning {
			continue
		}
		// Condition A: The worker assigned to this job was just marked dead.
		_, workerDead := dead[j.AssignedWorker]

		// Condition B: The lease time has passed.
		// Even if the worker is "alive" (sending heartbeats), it might be stuck or slow on this specific job.
		leaseExpired := !j.LeaseExpiresAt.IsZero() && now.After(j.LeaseExpiresAt)

		// In either case, we must recover the job to ensure it eventually gets done.
		if workerDead || leaseExpired {
			reason := "lease expired"
			if workerDead {
				reason = "worker dead"
			}

			// Transition to Requeued state so it can be picked up by PollWork
			j.State = JobRequeued
			j.AssignedWorker = ""
			j.LeaseExpiresAt = time.Time{}
			j.UpdatedAt = now
			j.LastMessage = "requeued: " + reason

			c.queue = append(c.queue, j.ID)
		}
	}
}

func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

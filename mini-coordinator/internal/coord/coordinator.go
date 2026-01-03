package coord

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	coordv1 "example.com/mini-coordinator/gen/example.com/mini-coordinator/gen/coordv1"
)

const (
	workerTimeout  = 6 * time.Second
	reaperInterval = 1 * time.Second
)

type JobState int

const (
	JobPending JobState = iota
	JobRunning
	JobSucceeded
	JobFailed
	JobRequeued
)

type Job struct {
	ID             string
	Payload        string
	State          JobState
	AssignedWorker string
	LeaseExpiresAt time.Time // The Coordinator says, "You have this job for 10 seconds. If I don't hear from you by then, I'm taking it back and giving it to someone else."
	LastMessage    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Attempts       int
}

// Worker - the citizen of the distributed system
type Worker struct {
	ID            string
	Hostname      string
	Labels        map[string]string
	LastHeartbeat time.Time
	RegisteredAt  time.Time
}

// Coordinator - the government of the distributed system. Has workers and jobs that it manages
type Coordinator struct {
	coordv1.UnimplementedCoordinatorServer
	mu sync.Mutex

	workers map[string]*Worker
	jobs    map[string]*Job
	queue   []string //jobIds in FIFO order

	heartbeatInterval time.Duration
	leaseDuration     time.Duration
}

// NewCoordinator initialize the coordinator
func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers:           make(map[string]*Worker),
		jobs:              make(map[string]*Job),
		queue:             make([]string, 0),
		heartbeatInterval: 2 * time.Second,
		leaseDuration:     10 * time.Second,
	}
}

/*
	Must follow the gRPC pattern of:

Receiver: pointer to a struct (e.g., *Coordinator).
First Argument: Always context.Context.
Second Argument: A pointer to the generated Request struct (e.g., *coordv1.RegisterWorkerRequest).
Return Values: A pointer to the generate Response struct and an error.
*/
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

// Heartbeat grpc call: request a heartbeat, returns a heartbeat response. This function simply keeps the worker alive by setting the new heartbeat to time.now()
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

// SubmitJob -
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

func (c *Coordinator) PollWork(ctx context.Context, req *coordv1.PollWorkRequest) (*coordv1.PollWorkResponse, error) {
	workerID := req.GetWorkerId()
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure worker exists
	if _, ok := c.workers[workerID]; !ok {
		return nil, fmt.Errorf("unknown worker_id: %s", workerID)
	}

	for len(c.queue) > 0 {
		jobID := c.queue[0]
		c.queue = c.queue[1:]
		j, ok := c.jobs[jobID]
		if !ok {
			continue
		}
		//find a pending job
		if j.State != JobPending && j.State != JobRequeued {
			continue
		}

		//found a pending job, set its parameters and assign to worker
		j.State = JobRunning
		j.AssignedWorker = workerID
		j.Attempts++
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

	return &coordv1.PollWorkResponse{
		Result: &coordv1.PollWorkResponse_NoJob{
			NoJob: &coordv1.NoJob{RetryAfterMs: 500},
		},
	}, nil
}

func (c *Coordinator) ReportWork(ctx context.Context, req *coordv1.ReportWorkRequest) (*coordv1.ReportWorkResponse, error) {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[req.GetJobId()]
	if !ok {
		return nil, fmt.Errorf("unknown job_id: %s", req.GetJobId())
	}

	// Fencing-lite: only the currently assigned worker can report
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

// RunReaper : background go routine to reap dead workers
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

func (c *Coordinator) reapDeadWorkers() {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	// find dead workers
	dead := make(map[string]struct{})
	for id, w := range c.workers {
		if now.Sub(w.LastHeartbeat) > workerTimeout {
			fmt.Printf("worker %s timed out (last hb %v ago)\n", id, now.Sub(w.LastHeartbeat))
			delete(c.workers, id)
		}
	}

	// reclaim jobs from dead workers or expired leases
	for _, j := range c.jobs {
		if j.State != JobRunning {
			continue
		}
		// see if the worker for this job is dead
		_, workerDead := dead[j.AssignedWorker]
		// see if lease for job has expired
		leaseExpired := !j.LeaseExpiresAt.IsZero() && now.After(j.LeaseExpiresAt)

		// either case reap the job and add it to queue
		if workerDead || leaseExpired {
			reason := "lease expired"
			if workerDead {
				reason = "worker dead"
			}

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

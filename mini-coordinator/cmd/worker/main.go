package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	coordv1 "example.com/mini-coordinator/gen/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var coordAddr string
	flag.StringVar(&coordAddr, "coord", "127.0.0.1:50051", "coordinator address")
	flag.Parse()

	// the pipe of the connection (the setup)
	// look contrary to our serve which setup and listen, this dials the server and connects to it
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// send data through that pipe
	c := coordv1.NewCoordinatorClient(conn)
	// if coordinator doesn't answer in 3 seconds, assume its dead
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// actual network request sending an 'API' with method register worker
	resp, err := c.RegisterWorker(
		ctx,
		&coordv1.RegisterWorkerRequest{Hostname: "worker-1"},
	)
	if err != nil {
		log.Printf("RegisterWorker (expected for now): %v", err)
		return
	}
	fmt.Printf("registered worker_id=%s hb_interval_ms=%d\n", resp.WorkerId, resp.HeartbeatIntervalMs)

	// add heartbeat loop
	workerID := resp.WorkerId
	hbInterval := time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond

	// report heartbeat in background thread
	go func() {
		ticker := time.NewTicker(hbInterval)
		defer ticker.Stop()

		for {
			_, err := c.Heartbeat(context.Background(), &coordv1.HeartbeatRequest{WorkerId: workerID})
			if err != nil {
				log.Printf("heartbeat error: %v", err)
			}
			time.Sleep(hbInterval)
		}
	}()

	// complete work in main
	for {
		resp, err := c.PollWork(context.Background(), &coordv1.PollWorkRequest{WorkerId: workerID})
		if err != nil {
			log.Printf("PollWork error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		switch r := resp.Result.(type) {
		case *coordv1.PollWorkResponse_NoJob:
			time.Sleep(time.Duration(r.NoJob.RetryAfterMs) * time.Millisecond)

		case *coordv1.PollWorkResponse_Job:
			job := r.Job
			attempt := job.Attempt
			log.Printf("got job %s payload=%q lease=%dms attempt=%d",
				job.JobId, job.Payload, job.LeaseDurationMs, job.Attempt)

			// Execute: interpret payload like "sleep_ms=1200"
			err := executePayload(job.Payload)

			status := coordv1.WorkStatus_WORK_STATUS_SUCCEEDED
			msg := "ok"
			if err != nil {
				status = coordv1.WorkStatus_WORK_STATUS_FAILED
				msg = err.Error()
			}

			_, repErr := c.ReportWork(context.Background(), &coordv1.ReportWorkRequest{
				WorkerId: workerID,
				JobId:    job.JobId,
				Status:   status,
				Message:  msg,
				Attempt:  attempt,
			})
			if repErr != nil {
				log.Printf("ReportWork error: %v", repErr)
			}

		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// this can be a call to an api to do work or anything really
func executePayload(payload string) error {
	// payload format: "sleep_ms=1200"
	const prefix = "sleep_ms="
	if !strings.HasPrefix(payload, prefix) {
		return fmt.Errorf("unknown payload: %q", payload)
	}
	msStr := strings.TrimPrefix(payload, prefix)
	ms, err := strconv.Atoi(msStr)
	if err != nil {
		return fmt.Errorf("bad sleep_ms: %v", err)
	}
	// the actual work currently is sleeping
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return nil
}

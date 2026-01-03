package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	coordv1 "example.com/mini-coordinator/mini-coordinator/gen/example.com/mini-coordinator/gen/coordv1"
	psv1 "example.com/mini-coordinator/mini-coordinator/gen/example.com/mini-coordinator/gen/psv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// main is the entry point for the Worker process.
// It connects to the Coordinator, registers itself, and enters a loop to poll for and execute work.
func main() {
	var coordAddr string
	flag.StringVar(&coordAddr, "coord", "127.0.0.1:50051", "coordinator address")

	var shardIdx int
	var shardTotal int
	flag.IntVar(&shardIdx, "shard_idx", 0, "shard index (0-based)")
	flag.IntVar(&shardTotal, "shard_total", 1, "total shards")
	flag.Parse()

	// Establish a connection to the Coordinator.
	// We use insecure credentials for simplicity in this demo.
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Create clients for both the Coordinator and Parameter Server services.
	c := coordv1.NewCoordinatorClient(conn)
	ps := psv1.NewParameterServerClient(conn)

	// Register with the Coordinator.
	// We set a short timeout to fail fast if the coordinator is unreachable.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.RegisterWorker(
		ctx,
		&coordv1.RegisterWorkerRequest{Hostname: "worker-1"},
	)
	if err != nil {
		log.Printf("RegisterWorker (expected for now): %v", err)
		return
	}
	fmt.Printf("registered worker_id=%s hb_interval_ms=%d\n", resp.WorkerId, resp.HeartbeatIntervalMs)

	// Start the heartbeat loop.
	workerID := resp.WorkerId
	hbInterval := time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond

	// Run heartbeat reporting in a background goroutine.
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

	// Main work loop: continuously poll for jobs.
	for {
		resp, err := c.PollWork(context.Background(), &coordv1.PollWorkRequest{WorkerId: workerID})
		if err != nil {
			log.Printf("PollWork error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		switch r := resp.Result.(type) {
		case *coordv1.PollWorkResponse_NoJob:
			// No work available, back off for the requested duration.
			time.Sleep(time.Duration(r.NoJob.RetryAfterMs) * time.Millisecond)

		case *coordv1.PollWorkResponse_Job:
			// Received a job assignment.
			job := r.Job
			attempt := job.Attempt
			log.Printf("got job %s payload=%q lease=%dms attempt=%d",
				job.JobId, job.Payload, job.LeaseDurationMs, job.Attempt)

			// Execute the job payload.
			// This function interprets the payload string and performs the requested task (e.g., training).
			err := executePayload(job.Payload, ps, shardIdx, shardTotal)

			status := coordv1.WorkStatus_WORK_STATUS_SUCCEEDED
			msg := "ok"
			if err != nil {
				status = coordv1.WorkStatus_WORK_STATUS_FAILED
				msg = err.Error()
			}

			// Report the result back to the Coordinator.
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

// executePayload interprets the job payload string and executes the corresponding logic.
// Supported commands:
// - "ps_train": Runs distributed training using the Parameter Server.
// - "sleep_ms": Simulates work by sleeping.
func executePayload(payload string, psClient psv1.ParameterServerClient, shardIdx int, shardTotal int) error {

	kind, kv, err := parseKVPayload(payload)
	if err != nil {
		return err
	}
	switch kind {
	case "ps_train":
		return runPSTrain(payload, kv, psClient, shardIdx, shardTotal)
	case "sleep_ms":
		ms, err := strconv.Atoi(kv["ms"])
		if err != nil {
			return fmt.Errorf("bad sleep_ms: %v", err)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	default:
		return fmt.Errorf("unknown payload kind: %q", payload)
	}
	return nil
}

// parseKVPayload parses strings in the format "command:key1=val1 key2=val2".
func parseKVPayload(s string) (string, map[string]string, error) {
	// split on the colon
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("bad payload format")
	}
	kind := parts[0]
	kv := map[string]string{}
	fields := strings.Fields(parts[1])
	for _, f := range fields {
		kvp := strings.SplitN(f, "=", 2)
		if len(kvp) != 2 {
			return "", nil, fmt.Errorf("bad kv: %q", f)
		}
		kv[kvp[0]] = kvp[1]
	}
	return kind, kv, nil
}

// runPSTrain performs a distributed training task (Linear Regression).
// It coordinates with the Parameter Server to pull weights and push gradients.
func runPSTrain(
	payload string,
	kv map[string]string,
	psClient psv1.ParameterServerClient,
	shardIdx int,
	shardTotal int,
) error {
	modelID := kv["model_id"]
	if modelID == "" {
		return fmt.Errorf("ps_train requires model_id")
	}

	dim, err := strconv.Atoi(kv["dim"])
	if err != nil || dim <= 0 {
		return fmt.Errorf("ps_train requires dim>0")
	}

	steps, err := strconv.Atoi(kv["steps"])
	if err != nil || steps <= 0 {
		return fmt.Errorf("ps_train requires steps>0")
	}

	lr64, err := strconv.ParseFloat(kv["lr"], 32)
	if err != nil || lr64 <= 0 {
		return fmt.Errorf("ps_train requires lr64>0")
	}

	lr := float32(lr64)

	// Ensure the model exists on the PS. If not, initialize it.
	_, pullErr := psClient.PullWeights(context.Background(), &psv1.PullWeightsRequest{ModelId: modelID})
	if pullErr != nil {
		// Initialize the model with zero weights.
		// In a real system, you might want random initialization (e.g., Xavier/He).
		w0 := make([]float32, dim)
		_, initErr := psClient.InitModel(context.Background(), &psv1.InitModelRequest{ModelId: modelID, Weights: w0})
		if initErr != nil {
			return fmt.Errorf("model init failed: %v", initErr)
		}
	}

	// Synthetic dataset parameters (deterministic).
	// "True" weights are unknown to the model but fixed so we can calculate loss.
	trueW := make([]float32, dim)
	for i := range trueW {
		trueW[i] = float32(i+1) / float32(dim)
	}

	// Each worker owns a shard of sample indices.
	// We generate samples on the fly: x is deterministic from (sample_id, feature).
	numSamples := 2000
	start := (numSamples * shardIdx) / shardTotal
	end := (numSamples * (shardIdx + 1)) / shardTotal
	if start >= end {
		return fmt.Errorf("empty shard: idx=%d total=%d", shardIdx, shardTotal)
	}

	// Training loop
	for step := 1; step <= steps; step++ {
		// 1. Pull current weights from Parameter Server
		pw, err := psClient.PullWeights(context.Background(), &psv1.PullWeightsRequest{ModelId: modelID})
		if err != nil {
			return fmt.Errorf("PullWeights failed: %v", err)
		}
		w := pw.Weights
		if len(w) != dim {
			return fmt.Errorf("weight dim mismatch: got %d want %d", len(w), dim)
		}

		// 2. Compute gradient of MSE over this shard
		grad := make([]float32, dim)
		var loss float32

		for s := start; s < end; s++ {
			x := make([]float32, dim)
			for j := 0; j < dim; j++ {
				// deterministic "random-ish" feature generation
				x[j] = float32(((s+1)*(j+7))%13) / 13.0
			}
			// y = trueW·x (Target)
			var y float32
			for j := 0; j < dim; j++ {
				y += trueW[j] * x[j]
			}
			// yhat = w·x (Prediction)
			var yhat float32
			for j := 0; j < dim; j++ {
				yhat += w[j] * x[j]
			}
			errVal := (yhat - y)
			loss += errVal * errVal

			// grad += 2/N * err * x
			for j := 0; j < dim; j++ {
				grad[j] += errVal * x[j]
			}
		}

		// Normalize loss and gradients by the number of samples in the shard.
		n := float32(end - start)
		loss = loss / n
		scale := float32(2.0) / n
		for j := 0; j < dim; j++ {
			grad[j] *= scale
		}

		// 3. Push gradients to Parameter Server (it applies the SGD update)
		_, err = psClient.PushGradients(context.Background(), &psv1.PushGradientsRequest{
			ModelId:      modelID,
			Gradients:    grad,
			LearningRate: lr,
		})
		if err != nil {
			return fmt.Errorf("PushGradients failed: %v", err)
		}

		// Log progress occasionally.
		if step%20 == 0 {
			log.Printf("ps_train model=%s shard=%d/%d step=%d loss=%.6f",
				modelID, shardIdx, shardTotal, step, loss)
		}
	}

	return nil
}

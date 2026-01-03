package ps

import (
	"context"
	"fmt"
	"sync"
	"time"

	psv1 "example.com/mini-coordinator/mini-coordinator/gen/example.com/mini-coordinator/gen/psv1"
)

// Model represents a machine learning model stored in the Parameter Server.
// It contains the weights, versioning info, and metadata.
type Model struct {
	ID        string
	Weights   []float32
	Version   int64 // Monotonic version counter, incremented on every update.
	UpdatedAt time.Time
}

// Server implements the ParameterServer gRPC service.
// It manages a collection of models in memory and handles concurrent access.
type Server struct {
	psv1.UnimplementedParameterServerServer

	mu     sync.Mutex
	models map[string]*Model
}

// NewServer creates a new Parameter Server instance.
func NewServer() *Server {
	return &Server{
		models: make(map[string]*Model),
	}
}

// InitModel initializes a new model with the given ID and weights.
// If the model already exists, it is overwritten (in this simple implementation).
func (s *Server) InitModel(ctx context.Context, req *psv1.InitModelRequest) (*psv1.InitModelResponse, error) {
	if req.GetModelId() == "" {
		return nil, fmt.Errorf("model_id is required")
	}
	if len(req.GetWeights()) == 0 {
		return nil, fmt.Errorf("weights must be non-empty")
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Important: Copy the weights from the request.
	// We cannot store the slice from the request directly because the underlying array
	// might be reused by the gRPC library or caller, leading to race conditions or data corruption.
	w := make([]float32, len(req.Weights))
	copy(w, req.Weights)

	s.models[req.ModelId] = &Model{
		ID:        req.ModelId,
		Weights:   w,
		Version:   1,
		UpdatedAt: now,
	}

	return &psv1.InitModelResponse{
		Version: 1,
	}, nil

}

// PullWeights retrieves the current weights for a requested model.
// It supports a simple optimization where the server can return empty weights
// if the client's version is already up to date.
func (s *Server) PullWeights(ctx context.Context, req *psv1.PullWeightsRequest) (*psv1.PullWeightsResponse, error) {
	if req.GetModelId() == "" {
		return nil, fmt.Errorf("model_id is required")
	}

	s.mu.Lock()
	m, ok := s.models[req.ModelId]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("unknown model_id: %s", req.ModelId)
	}

	// Optimization: If the requester asks for "min_version X" and our current version is <= X,
	// it means they already have the latest data (or newer). We can save bandwidth.
	if req.GetMinVersion() != 0 && m.Version <= req.GetMinVersion() {
		v := m.Version
		s.mu.Unlock()
		return &psv1.PullWeightsResponse{Weights: nil, Version: v}, nil
	}

	// Copy the weights to avoid race conditions if a write happens while we are returning.
	out := make([]float32, len(m.Weights))
	copy(out, m.Weights)
	v := m.Version
	s.mu.Unlock()

	return &psv1.PullWeightsResponse{
		Weights: out,
		Version: v,
	}, nil

}

// PushGradients updates the model weights using the provided gradients and learning rate.
// It implements a standard SGD step: weights = weights - (learning_rate * gradients).
// This operation is atomic; the lock is held during the update.
func (s *Server) PushGradients(ctx context.Context, req *psv1.PushGradientsRequest) (*psv1.PushGradientsResponse, error) {
	if req.GetModelId() == "" {
		return nil, fmt.Errorf("model_id is required")
	}
	if req.GetLearningRate() <= 0 {
		return nil, fmt.Errorf("learning_rate must be > 0")
	}
	if len(req.GetGradients()) == 0 {
		return nil, fmt.Errorf("gradients must be non-empty")
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.models[req.ModelId]
	if !ok {
		return nil, fmt.Errorf("unknown model_id: %s", req.ModelId)
	}
	if len(req.Gradients) != len(m.Weights) {
		return nil, fmt.Errorf("gradient dim %d != weight dim %d", len(req.Gradients), len(m.Weights))
	}

	lr := req.LearningRate
	// SGD update loop.
	// Note: In a real production system, this would use BLAS or SIMD instructions for performance.
	for i := range m.Weights {
		m.Weights[i] -= lr * req.Gradients[i]
	}

	m.Version++
	m.UpdatedAt = now

	return &psv1.PushGradientsResponse{NewVersion: m.Version}, nil
}

// GetPSStatus returns a summary of all models hosted on this server.
func (s *Server) GetPSStatus(ctx context.Context, req *psv1.GetPSStatusRequest) (*psv1.GetPSStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &psv1.GetPSStatusResponse{}
	for _, m := range s.models {
		out.Models = append(out.Models, &psv1.ModelStatus{
			ModelId: m.ID,
			Version: m.Version,
			Dim:     int32(len(m.Weights)),
		})
	}
	return out, nil
}

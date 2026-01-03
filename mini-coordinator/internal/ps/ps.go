package ps

import (
	"context"
	"fmt"
	"sync"
	"time"

	psv1 "example.com/mini-coordinator/gen/example.com/mini-coordinator/gen/psv1"
)

type Model struct {
	ID        string
	Weights   []float32
	Version   int64
	UpdatedAt time.Time
}

type Server struct {
	psv1.UnimplementedParameterServerServer

	mu     sync.Mutex
	models map[string]*Model
}

func NewServer() *Server {
	return &Server{
		models: make(map[string]*Model),
	}
}

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

	// ❌ WRONG - storing the slice directly
	// s.modelWeights = req.Weights
	// You now have a pointer to memory that will be reused!
	// Never trust the memory passed in by a request; you copy it to your own safe
	// "vault" so it can't be changed under your feet.
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

	//Hey, I have Version 5, and the server also has Version 5,
	// the server sends back nil weights
	if req.GetMinVersion() != 0 && m.Version <= req.GetMinVersion() {
		v := m.Version
		s.mu.Unlock()
		return &psv1.PullWeightsResponse{Weights: nil, Version: v}, nil
	}

	// copy the weights
	out := make([]float32, len(m.Weights))
	copy(out, m.Weights)
	v := m.Version
	s.mu.Unlock()
	return &psv1.PullWeightsResponse{
		Weights: out,
		Version: v,
	}, nil

}

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
	// SGD update: w = w - lr * grad
	for i := range m.Weights {
		m.Weights[i] -= lr * req.Gradients[i]
	}

	m.Version++
	m.UpdatedAt = now

	return &psv1.PushGradientsResponse{NewVersion: m.Version}, nil
}

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

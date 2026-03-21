package models

import (
	"fmt"
	"sync"
	"time"

	ort "github.com/yalue/onnxruntime_go"
)

// ModelSession wraps an ONNX runtime session with synchronization.
// Sessions are not goroutine-safe in onnxruntime, so a mutex serializes access.
type ModelSession struct {
	mu       sync.Mutex
	session  *ort.AdvancedSession
	config   ModelConfig
	lastUsed time.Time
}

// newModelSession creates and initializes an ONNX session from the given config.
func newModelSession(cfg ModelConfig) (*ModelSession, error) {
	inputs := make([]ort.ArbitraryTensor, len(cfg.InputNames))
	outputs := make([]ort.ArbitraryTensor, len(cfg.OutputNames))

	// Create placeholder input tensor (shape from config, float32)
	shape := ort.NewShape(cfg.InputShape...)
	size := int64(1)
	for _, d := range cfg.InputShape {
		size *= d
	}
	inputData := make([]float32, size)
	inputTensor, err := ort.NewTensor(shape, inputData)
	if err != nil {
		return nil, fmt.Errorf("create input tensor: %w", err)
	}
	for i := range inputs {
		inputs[i] = inputTensor
	}

	// Create placeholder output tensor (single float32 output)
	outputShape := ort.NewShape(1, 1)
	outputData := make([]float32, 1)
	outputTensor, err := ort.NewTensor(outputShape, outputData)
	if err != nil {
		inputTensor.Destroy()
		return nil, fmt.Errorf("create output tensor: %w", err)
	}
	for i := range outputs {
		outputs[i] = outputTensor
	}

	session, err := ort.NewAdvancedSession(
		cfg.Path,
		cfg.InputNames,
		cfg.OutputNames,
		inputs,
		outputs,
		nil,
	)
	if err != nil {
		inputTensor.Destroy()
		outputTensor.Destroy()
		return nil, fmt.Errorf("create ONNX session for %s: %w", cfg.Name, err)
	}

	return &ModelSession{
		session:  session,
		config:   cfg,
		lastUsed: time.Now(),
	}, nil
}

// Run executes the ONNX session with the given input data and returns the output.
// Callers must ensure input length matches the configured InputShape.
func (ms *ModelSession) Run(input []float32) ([]float32, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.lastUsed = time.Now()

	shape := ort.NewShape(ms.config.InputShape...)
	inputTensor, err := ort.NewTensor(shape, input)
	if err != nil {
		return nil, fmt.Errorf("create input tensor: %w", err)
	}
	defer inputTensor.Destroy()

	outputShape := ort.NewShape(1, 1)
	output := make([]float32, 1)
	outputTensor, err := ort.NewTensor(outputShape, output)
	if err != nil {
		return nil, fmt.Errorf("create output tensor: %w", err)
	}
	defer outputTensor.Destroy()

	err = ms.session.Run()
	if err != nil {
		return nil, fmt.Errorf("run ONNX session %s: %w", ms.config.Name, err)
	}

	return output, nil
}

// RunBatch executes the ONNX session with a batch of inputs.
// batchSize is the number of rows, featureSize is the number of features per row.
func (ms *ModelSession) RunBatch(input []float32, batchSize, featureSize int) ([]float32, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.lastUsed = time.Now()

	inputShape := ort.NewShape(int64(batchSize), int64(featureSize))
	inputTensor, err := ort.NewTensor(inputShape, input)
	if err != nil {
		return nil, fmt.Errorf("create batch input tensor: %w", err)
	}
	defer inputTensor.Destroy()

	outputShape := ort.NewShape(int64(batchSize), 1)
	output := make([]float32, batchSize)
	outputTensor, err := ort.NewTensor(outputShape, output)
	if err != nil {
		return nil, fmt.Errorf("create batch output tensor: %w", err)
	}
	defer outputTensor.Destroy()

	err = ms.session.Run()
	if err != nil {
		return nil, fmt.Errorf("run ONNX batch session %s: %w", ms.config.Name, err)
	}

	return output, nil
}

// Close destroys the ONNX session.
func (ms *ModelSession) Close() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.session != nil {
		ms.session.Destroy()
		ms.session = nil
	}
}

// LastUsed returns the time of last Run call.
func (ms *ModelSession) LastUsed() time.Time {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.lastUsed
}

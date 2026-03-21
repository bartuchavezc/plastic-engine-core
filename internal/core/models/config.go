package models

// ModelConfig describes a model that can be loaded into the ModelStore.
type ModelConfig struct {
	// Name is the unique identifier for this model (e.g. "edge_weight_v1").
	Name string

	// Path is the filesystem path to the model.onnx file.
	Path string

	// InputNames are the ONNX input tensor names.
	InputNames []string

	// OutputNames are the ONNX output tensor names.
	OutputNames []string

	// InputShape describes the expected input dimensions (e.g. []int64{1, 6}).
	InputShape []int64

	// Preload loads the model at registration time instead of on first use.
	Preload bool
}

package indexes

import "errors"

var (
	// ErrIndexIDExists is returned when the provided index ID already exists.
	ErrIndexIDExists = errors.New("index id already exists")
	// ErrIndexNameExists is returned when another index uses the same name.
	ErrIndexNameExists = errors.New("index name already exists")
	// ErrIndexNotFound signals a missing index definition.
	ErrIndexNotFound = errors.New("index not found")
)

// ValidationError indicates that the provided index request is invalid.
type ValidationError struct {
	msg string
}

// Error makes ValidationError satisfy the error interface.
func (e *ValidationError) Error() string {
	return e.msg
}

// NewValidationError builds a new validation error.
func NewValidationError(message string) *ValidationError {
	return &ValidationError{msg: message}
}

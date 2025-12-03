package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

// Field represents a structured log attribute.
type Field struct {
	Key   string
	Value any
}

// Logger defines the minimal logging contract used across the project.
type Logger interface {
	Info(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Debug(msg string, fields ...Field)
}

var (
	globalLogger Logger
	initOnce     sync.Once
)

// DefaultLogger returns the lazily initialised application logger.
// TODO: replace with OpenTelemetry logger provider.
func DefaultLogger() Logger {
	initOnce.Do(func() {
		base := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)
		globalLogger = &stdLogger{base: base}
	})
	return globalLogger
}

type stdLogger struct {
	base *log.Logger
}

func (l *stdLogger) Info(msg string, fields ...Field) {
	l.base.Printf("[INFO] %s %s", msg, formatFields(fields))
}

func (l *stdLogger) Error(msg string, fields ...Field) {
	l.base.Printf("[ERROR] %s %s", msg, formatFields(fields))
}

func (l *stdLogger) Debug(msg string, fields ...Field) {
	l.base.Printf("[DEBUG] %s %s", msg, formatFields(fields))
}

func formatFields(fields []Field) string {
	if len(fields) == 0 {
		return ""
	}

	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		parts = append(parts, fmt.Sprintf("%s=%v", f.Key, f.Value))
	}

	return strings.Join(parts, " ")
}

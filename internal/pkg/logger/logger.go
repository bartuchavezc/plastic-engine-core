package logger

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/trace"
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
	Warn(msg string, fields ...Field)

	// Context-aware methods that automatically include trace_id and span_id
	InfoCtx(ctx context.Context, msg string, fields ...Field)
	ErrorCtx(ctx context.Context, msg string, fields ...Field)
	DebugCtx(ctx context.Context, msg string, fields ...Field)
	WarnCtx(ctx context.Context, msg string, fields ...Field)

	// With returns a logger with additional base fields
	With(fields ...Field) Logger
}

var (
	globalLogger Logger
	initOnce     sync.Once
)

// DefaultLogger returns the lazily initialised application logger.
func DefaultLogger() Logger {
	initOnce.Do(func() {
		base := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)
		globalLogger = &stdLogger{base: base}
	})
	return globalLogger
}

type stdLogger struct {
	base       *log.Logger
	baseFields []Field
}

func (l *stdLogger) Info(msg string, fields ...Field) {
	l.log("INFO", msg, fields)
}

func (l *stdLogger) Error(msg string, fields ...Field) {
	l.log("ERROR", msg, fields)
}

func (l *stdLogger) Debug(msg string, fields ...Field) {
	l.log("DEBUG", msg, fields)
}

func (l *stdLogger) Warn(msg string, fields ...Field) {
	l.log("WARN", msg, fields)
}

func (l *stdLogger) InfoCtx(ctx context.Context, msg string, fields ...Field) {
	l.logCtx(ctx, "INFO", msg, fields)
}

func (l *stdLogger) ErrorCtx(ctx context.Context, msg string, fields ...Field) {
	l.logCtx(ctx, "ERROR", msg, fields)
}

func (l *stdLogger) DebugCtx(ctx context.Context, msg string, fields ...Field) {
	l.logCtx(ctx, "DEBUG", msg, fields)
}

func (l *stdLogger) WarnCtx(ctx context.Context, msg string, fields ...Field) {
	l.logCtx(ctx, "WARN", msg, fields)
}

func (l *stdLogger) With(fields ...Field) Logger {
	newFields := make([]Field, 0, len(l.baseFields)+len(fields))
	newFields = append(newFields, l.baseFields...)
	newFields = append(newFields, fields...)
	return &stdLogger{
		base:       l.base,
		baseFields: newFields,
	}
}

func (l *stdLogger) log(level string, msg string, fields []Field) {
	allFields := l.mergeFields(fields)
	l.base.Printf("[%s] %s %s", level, msg, formatFields(allFields))
}

func (l *stdLogger) logCtx(ctx context.Context, level string, msg string, fields []Field) {
	allFields := l.mergeFields(fields)

	// Extract trace context if available
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		allFields = append([]Field{
			{Key: "trace_id", Value: spanCtx.TraceID().String()},
			{Key: "span_id", Value: spanCtx.SpanID().String()},
		}, allFields...)
	}

	l.base.Printf("[%s] %s %s", level, msg, formatFields(allFields))
}

func (l *stdLogger) mergeFields(fields []Field) []Field {
	if len(l.baseFields) == 0 {
		return fields
	}
	result := make([]Field, 0, len(l.baseFields)+len(fields))
	result = append(result, l.baseFields...)
	result = append(result, fields...)
	return result
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

// TraceField creates a Field for trace_id from context.
func TraceField(ctx context.Context) Field {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		return Field{Key: "trace_id", Value: spanCtx.TraceID().String()}
	}
	return Field{Key: "trace_id", Value: ""}
}

// SpanField creates a Field for span_id from context.
func SpanField(ctx context.Context) Field {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		return Field{Key: "span_id", Value: spanCtx.SpanID().String()}
	}
	return Field{Key: "span_id", Value: ""}
}

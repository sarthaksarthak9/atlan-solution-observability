package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	tracer trace.Tracer
	logger *zap.SugaredLogger
)

// initTracer sets up the OpenTelemetry tracing
func initTracer() (func(context.Context) error, error) {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("atlan-api-service"),
			attribute.String("environment", "production"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Set up the OTLP exporter
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint("otel-collector:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	// Set up the trace provider
	bsp := sdktrace.NewBatchSpanProcessor(exporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)
	tracer = tracerProvider.Tracer("atlan-api-service")

	// Return a cleanup function
	return func(ctx context.Context) error {
		return tracerProvider.Shutdown(ctx)
	}, nil
}

// initLogger sets up structured logging
func initLogger() (*zap.SugaredLogger, error) {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.StacktraceKey = "stacktrace"

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}
	return logger.Sugar(), nil
}

// tracingMiddleware adds tracing to incoming requests
func tracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = fmt.Sprintf("%d", time.Now().UnixNano())
		}

		ctx, span := tracer.Start(ctx, r.URL.Path,
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.request_id", requestID),
				attribute.String("http.user_agent", r.UserAgent()),
			),
		)
		defer span.End()

		// Add trace context to logger
		ctxLogger := logger.With(
			"trace_id", span.SpanContext().TraceID().String(),
			"span_id", span.SpanContext().SpanID().String(),
			"request_id", requestID,
		)

		// Log request
		ctxLogger.Infow("Request received",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
		)

		// Create a custom response writer to capture status code
		rw := newResponseWriter(w)

		start := time.Now()
		next.ServeHTTP(rw, r.WithContext(ctx))
		duration := time.Since(start)

		// Add response attributes to span
		span.SetAttributes(
			attribute.Int("http.status_code", rw.statusCode),
			attribute.Int64("http.response_time_ms", duration.Milliseconds()),
		)

		// Log response
		ctxLogger.Infow("Request completed",
			"duration_ms", duration.Milliseconds(),
			"status_code", rw.statusCode,
			"status", statusLabel(rw.statusCode),
		)
	})
}

// responseWriter is a wrapper for http.ResponseWriter that captures the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// statusLabel returns a label for the HTTP status code
func statusLabel(code int) string {
	if code >= 200 && code < 300 {
		return "success"
	} else if code >= 400 && code < 500 {
		return "client_error"
	} else if code >= 500 {
		return "server_error"
	}
	return "unknown"
}

// simulateDBQuery performs a database query with tracing
func simulateDBQuery(ctx context.Context, query string, params map[string]interface{}) ([]map[string]interface{}, error) {
	ctx, span := tracer.Start(ctx, "database.query",
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.statement", query),
		),
	)
	defer span.End()

	// Extract logger with trace context
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()
	ctxLogger := logger.With(
		"trace_id", traceID,
		"span_id", spanID,
	)

	// Log parameters safely
	ctxLogger.Debugw("Executing database query",
		"query", query,
		"params", sanitizeParams(params),
	)

	// Simulate a database operation with variable latency
	queryTime := simulateQueryTime()
	time.Sleep(queryTime)

	// Record the query time
	span.SetAttributes(attribute.Int64("db.execution_time_ms", queryTime.Milliseconds()))

	// Simulate occasional errors based on query time (slower queries more likely to fail)
	if queryTime > 200*time.Millisecond && time.Now().UnixNano()%10 == 0 {
		err := fmt.Errorf("database query timeout after %v", queryTime)
		span.RecordError(err)
		ctxLogger.Errorw("Database query failed",
			"error", err,
			"duration_ms", queryTime.Milliseconds(),
		)
		return nil, err
	}

	// Return mock results
	result := []map[string]interface{}{
		{"id": 1, "name": "Data item 1", "created_at": time.Now().Add(-24 * time.Hour)},
		{"id": 2, "name": "Data item 2", "created_at": time.Now().Add(-12 * time.Hour)},
	}

	ctxLogger.Debugw("Database query completed",
		"rows_returned", len(result),
		"duration_ms", queryTime.Milliseconds(),
	)

	return result, nil
}

// simulateQueryTime returns a simulated query execution time
func simulateQueryTime() time.Duration {
	// Base time plus some variability
	baseTime := 50 * time.Millisecond
	variability := time.Duration(time.Now().UnixNano()%200) * time.Millisecond
	return baseTime + variability
}

// sanitizeParams returns a safe version of query parameters for logging
func sanitizeParams(params map[string]interface{}) map[string]interface{} {
	sanitized := make(map[string]interface{})
	for k, v := range params {
		// Mask sensitive fields
		if k == "password" || k == "token" || k == "secret" {
			sanitized[k] = "****"
		} else {
			sanitized[k] = v
		}
	}
	return sanitized
}

// simulateExternalAPICall simulates an external API call with tracing
func simulateExternalAPICall(ctx context.Context, endpoint string) error {
	ctx, span := tracer.Start(ctx, "external.api.call",
		trace.WithAttributes(
			attribute.String("http.url", endpoint),
			attribute.String("http.method", "GET"),
		),
	)
	defer span.End()

	ctxLogger := logger.With(
		"trace_id", span.SpanContext().TraceID().String(),
		"span_id", span.SpanContext().SpanID().String(),
	)

	ctxLogger.Debugw("Calling external API", "endpoint", endpoint)

	// Simulate API call with variable latency
	callTime := 100 * time.Millisecond
	if time.Now().UnixNano()%3 == 0 {
		// Occasionally simulate slower calls
		callTime = 300 * time.Millisecond
	}
	time.Sleep(callTime)

	// Record the call time
	span.SetAttributes(attribute.Int64("http.duration_ms", callTime.Milliseconds()))

	// Simulate occasional errors
	if callTime > 200*time.Millisecond && time.Now().UnixNano()%8 == 0 {
		err := fmt.Errorf("external API timeout after %v", callTime)
		span.RecordError(err)
		ctxLogger.Errorw("External API call failed",
			"error", err,
			"duration_ms", callTime.Milliseconds(),
		)
		return err
	}

	ctxLogger.Debugw("External API call completed", "duration_ms", callTime.Milliseconds())
	return nil
}

// simulateCacheOperation simulates Redis cache operations with tracing
func simulateCacheOperation(ctx context.Context, operation, key string) (interface{}, bool, error) {
	ctx, span := tracer.Start(ctx, "cache.operation",
		trace.WithAttributes(
			attribute.String("cache.operation", operation),
			attribute.String("cache.key", key),
		),
	)
	defer span.End()

	ctxLogger := logger.With(
		"trace_id", span.SpanContext().TraceID().String(),
		"span_id", span.SpanContext().SpanID().String(),
	)

	ctxLogger.Debugw("Cache operation",
		"operation", operation,
		"key", key,
	)

	// Simulate cache operation
	time.Sleep(20 * time.Millisecond)

	// Simulate cache hit/miss (70% hit rate)
	hit := time.Now().UnixNano()%10 < 7
	span.SetAttributes(attribute.Bool("cache.hit", hit))

	value := map[string]interface{}{
		"cached_data": "value",
		"timestamp":   time.Now().Unix(),
	}

	if !hit {
		value = nil
		ctxLogger.Debugw("Cache miss", "key", key)
	} else {
		ctxLogger.Debugw("Cache hit", "key", key)
	}

	return value, hit, nil
}

// apiHandler processes API requests
func apiHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Extract the current span from context
	span := trace.SpanFromContext(ctx)

	// Get contextual logger
	ctxLogger := logger.With(
		"trace_id", span.SpanContext().TraceID().String(),
		"span_id", span.SpanContext().SpanID().String(),
	)

	// Parse request parameters
	r.ParseForm()
	userID := r.Form.Get("user_id")
	if userID != "" {
		span.SetAttributes(attribute.String("user.id", userID))
		ctxLogger = ctxLogger.With("user_id", userID)
	}

	ctxLogger.Infow("Processing API request",
		"params", r.Form,
	)

	// Try cache first
	cacheKey := fmt.Sprintf("user:%s:items", userID)
	cachedData, hit, _ := simulateCacheOperation(ctx, "GET", cacheKey)

	var results []map[string]interface{}
	var err error

	if !hit {
		// Cache miss, query database
		query := "SELECT * FROM items WHERE user_id = $1"
		params := map[string]interface{}{
			"user_id": userID,
		}

		results, err = simulateDBQuery(ctx, query, params)
		if err != nil {
			ctxLogger.Errorw("Database query failed", "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Store in cache for future requests
		simulateCacheOperation(ctx, "SET", cacheKey)
	} else {
		// Use cached data
		if cachedData != nil {
			results = []map[string]interface{}{cachedData.(map[string]interface{})}
		}
	}

	// Call external API for enrichment
	if err := simulateExternalAPICall(ctx, "https://api.example.com/enrich"); err != nil {
		ctxLogger.Warnw("Enrichment API call failed, continuing with basic data", "error", err)
		// Continue with basic data
	}

	// Process results
	ctxLogger.Infow("API request successful",
		"result_count", len(results),
		"cache_hit", hit,
	)

	// Set response headers
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Trace-ID", span.SpanContext().TraceID().String())
	w.WriteHeader(http.StatusOK)

	// Send response
	fmt.Fprintf(w, `{"status":"success","cache_hit":%v,"items":%d}`, hit, len(results))
}

func healthcheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"up"}`)
}

func main() {
	var err error

	// Initialize logger
	logger, err = initLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	// Initialize tracer
	cleanup, err := initTracer()
	if err != nil {
		logger.Fatalw("Failed to initialize tracer", "error", err)
	}
	defer cleanup(context.Background())

	// Set up HTTP server with tracing middleware
	http.Handle("/api/items", tracingMiddleware(http.HandlerFunc(apiHandler)))
	http.Handle("/health", http.HandlerFunc(healthcheckHandler))

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	logger.Infow("Starting server", "port", port)
	logger.Infow("Observability enabled",
		"tracing", true,
		"metrics", true,
		"structured_logging", true,
	)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		logger.Fatalw("Server failed", "error", err)
	}
}

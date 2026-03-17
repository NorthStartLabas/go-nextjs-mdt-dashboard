package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"extraction-pipeline/internal/db"
	"extraction-pipeline/internal/orchestrator"
)

// Server exposes HTTP endpoints for orchestrator control, stats, and configuration management.
type Server struct {
	sqlite      *db.SQLiteClient
	manager     *orchestrator.Manager
	log         *slog.Logger
	startTime   time.Time
	configFiles map[string]string
}

// NewServer creates a Server instance.
func NewServer(sqlite *db.SQLiteClient, manager *orchestrator.Manager, log *slog.Logger, configFiles map[string]string) *Server {
	return &Server{
		sqlite:      sqlite,
		manager:     manager,
		log:         log,
		startTime:   time.Now(),
		configFiles: configFiles,
	}
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/pipeline/run", s.handlePipelineRun)
	mux.HandleFunc("/api/pipeline/status", s.handlePipelineStatus)
	mux.HandleFunc("/api/pipeline/history", s.handlePipelineHistory)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/config/", s.handleConfig)
	return mux
}

func (s *Server) handlePipelineRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.methodNotAllowed(w, http.MethodPost)
		return
	}
	run, position, err := s.manager.Enqueue(r.Context())
	if err != nil {
		if errors.Is(err, orchestrator.ErrQueueFull) {
			s.writeError(w, http.StatusTooManyRequests, "pipeline queue is full", err)
			return
		}
		s.writeError(w, http.StatusInternalServerError, "failed to enqueue pipeline run", err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, map[string]any{
		"run":           runToDTO(run),
		"jobs_ahead":    position,
		"pending_limit": orchestrator.MaxPendingJobs,
	})
}

func (s *Server) handlePipelineStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, http.MethodGet)
		return
	}
	ctx := r.Context()
	activeRun, err := s.sqlite.GetActivePipelineRun(ctx)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to fetch active run", err)
		return
	}
	queuedRuns, err := s.sqlite.ListQueuedPipelineRuns(ctx)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to fetch queued runs", err)
		return
	}
	stats, err := s.sqlite.GetPipelineRunStats(ctx, time.Now().AddDate(0, 0, -30))
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to compute pipeline stats", err)
		return
	}
	recentRuns, err := s.sqlite.ListPipelineRunsSince(ctx, time.Now().AddDate(0, 0, -7), 50)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to fetch recent runs", err)
		return
	}

	resp := map[string]any{
		"pending_jobs":  s.manager.PendingCount(),
		"pending_limit": orchestrator.MaxPendingJobs,
		"queued_runs":   runsToDTOs(queuedRuns),
		"recent_runs":   runsToDTOs(recentRuns),
		"stats":         statsToDTO(stats),
	}
	if activeRun != nil {
		resp["active_run"] = runToDTO(*activeRun)
	}
	s.writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePipelineHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, http.MethodGet)
		return
	}
	days := 30
	if rawDays := r.URL.Query().Get("days"); rawDays != "" {
		if parsed, err := strconv.Atoi(rawDays); err == nil && parsed > 0 && parsed <= 365 {
			days = parsed
		}
	}
	since := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	runs, err := s.sqlite.ListPipelineRunsSince(r.Context(), since, 1000)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to fetch pipeline history", err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"days": days,
		"runs": runsToDTOs(runs),
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, http.MethodGet)
		return
	}
	hostname, _ := os.Hostname()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	status := "ok"
	if err := s.sqlite.Ping(ctx); err != nil {
		status = fmt.Sprintf("error: %v", err)
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"server_start":   s.startTime.UTC().Format(time.RFC3339Nano),
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"hostname":       hostname,
		"go_version":     runtime.Version(),
		"queue_depth":    s.manager.PendingCount(),
		"db_status":      status,
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/config/")
	if name == "" {
		http.NotFound(w, r)
		return
	}
	filePath, ok := s.configFiles[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleConfigGet(w, r, filePath)
	case http.MethodPut:
		s.handleConfigPut(w, r, filePath)
	default:
		s.methodNotAllowed(w, http.MethodGet, http.MethodPut)
	}
}

func (s *Server) handleConfigGet(w http.ResponseWriter, r *http.Request, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to read config", err)
		return
	}
	var parsed any
	if err := json.Unmarshal(data, &parsed); err != nil {
		s.writeError(w, http.StatusInternalServerError, "invalid config JSON", err)
		return
	}
	s.writeJSON(w, http.StatusOK, parsed)
}

func (s *Server) handleConfigPut(w http.ResponseWriter, r *http.Request, path string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "failed to read request body", err)
		return
	}
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		s.writeError(w, http.StatusBadRequest, "payload is not valid JSON", err)
		return
	}
	formatted, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to format JSON", err)
		return
	}
	if err := os.WriteFile(path, formatted, 0o644); err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to write config", err)
		return
	}
	s.writeJSON(w, http.StatusOK, parsed)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		s.log.Error("failed to write JSON response", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string, err error) {
	s.log.Error(message, "status", status, "error", err)
	s.writeJSON(w, status, map[string]any{
		"error":   message,
		"details": err.Error(),
	})
}

func (s *Server) methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	s.writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
		"error": fmt.Sprintf("method not allowed, use %s", strings.Join(allowed, ", ")),
	})
}

type pipelineRunDTO struct {
	ID              int64   `json:"id"`
	Status          string  `json:"status"`
	QueuedAt        string  `json:"queued_at"`
	StartedAt       *string `json:"started_at,omitempty"`
	FinishedAt      *string `json:"finished_at,omitempty"`
	DurationSeconds *int64  `json:"duration_seconds,omitempty"`
	ErrorMessage    string  `json:"error_message,omitempty"`
	RecordsPicking  *int64  `json:"records_picking,omitempty"`
	RecordsPacking  *int64  `json:"records_packing,omitempty"`
}

type pipelineStatsDTO struct {
	SuccessCount              int64    `json:"success_count"`
	FailureCount              int64    `json:"failure_count"`
	AvgSuccessDurationSeconds *float64 `json:"avg_success_duration_seconds,omitempty"`
	AvgFailureDurationSeconds *float64 `json:"avg_failure_duration_seconds,omitempty"`
}

func runToDTO(run db.PipelineRun) pipelineRunDTO {
	return pipelineRunDTO{
		ID:              run.ID,
		Status:          run.Status,
		QueuedAt:        run.QueuedAt.UTC().Format(time.RFC3339Nano),
		StartedAt:       timeToStringPtr(run.StartedAt),
		FinishedAt:      timeToStringPtr(run.FinishedAt),
		DurationSeconds: run.DurationSeconds,
		ErrorMessage:    run.ErrorMessage,
		RecordsPicking:  run.RecordsPicking,
		RecordsPacking:  run.RecordsPacking,
	}
}

func runsToDTOs(runs []db.PipelineRun) []pipelineRunDTO {
	result := make([]pipelineRunDTO, 0, len(runs))
	for _, r := range runs {
		result = append(result, runToDTO(r))
	}
	return result
}

func statsToDTO(stats db.PipelineRunStats) pipelineStatsDTO {
	return pipelineStatsDTO{
		SuccessCount:              stats.SuccessCount,
		FailureCount:              stats.FailureCount,
		AvgSuccessDurationSeconds: stats.AverageSuccessDuration,
		AvgFailureDurationSeconds: stats.AverageFailureDuration,
	}
}

func timeToStringPtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	formatted := t.UTC().Format(time.RFC3339Nano)
	return &formatted
}

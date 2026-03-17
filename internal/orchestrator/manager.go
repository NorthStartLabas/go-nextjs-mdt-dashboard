package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"extraction-pipeline/internal/db"
)

const (
	MaxPendingJobs = 5
)

// ErrQueueFull indicates the user attempted to enqueue beyond the maximum pending depth.
var ErrQueueFull = errors.New("pipeline queue is full")

// PipelineRunner abstracts how the pipeline is executed (binary, go run, etc.).
type PipelineRunner interface {
	Run(ctx context.Context) PipelineExecutionResult
}

// PipelineExecutionResult captures runtime information reported by the runner.
type PipelineExecutionResult struct {
	Duration       time.Duration
	Err            error
	ErrorMessage   string
	RecordsPicking *int64
	RecordsPacking *int64
}

// Manager serializes pipeline executions and tracks queue state.
type Manager struct {
	sqlite *db.SQLiteClient
	runner PipelineRunner
	log    *slog.Logger

	queueChan chan *pipelineJob
	mu        sync.Mutex
	pending   []*pipelineJob
	current   *pipelineJob
}

type pipelineJob struct {
	id       int64
	queuedAt time.Time
}

// NewManager builds a Manager instance and starts its worker loop.
func NewManager(sqlite *db.SQLiteClient, runner PipelineRunner, log *slog.Logger) *Manager {
	m := &Manager{
		sqlite:    sqlite,
		runner:    runner,
		log:       log,
		queueChan: make(chan *pipelineJob, MaxPendingJobs+1),
	}
	go m.worker()
	return m
}

// Enqueue schedules a new pipeline execution and returns its queue position (jobs ahead).
func (m *Manager) Enqueue(ctx context.Context) (db.PipelineRun, int, error) {
	now := time.Now().UTC()
	m.mu.Lock()
	if len(m.pending) >= MaxPendingJobs {
		m.mu.Unlock()
		return db.PipelineRun{}, 0, ErrQueueFull
	}
	jobsAhead := len(m.pending)
	if m.current != nil {
		jobsAhead++
	}
	m.mu.Unlock()

	insertCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	id, err := m.sqlite.InsertPipelineRun(insertCtx, now)
	if err != nil {
		return db.PipelineRun{}, 0, err
	}
	job := &pipelineJob{id: id, queuedAt: now}
	m.enqueueJob(job)
	return db.PipelineRun{ID: id, QueuedAt: now, Status: "queued"}, jobsAhead, nil
}

// PendingCount returns the number of jobs waiting to start (not counting the active job).
func (m *Manager) PendingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending)
}

func (m *Manager) enqueueJob(job *pipelineJob) {
	m.mu.Lock()
	m.pending = append(m.pending, job)
	m.mu.Unlock()
	m.queueChan <- job
}

func (m *Manager) worker() {
	for job := range m.queueChan {
		m.startJob(job)
		m.runJob(job)
		m.finishJob()
	}
}

func (m *Manager) startJob(job *pipelineJob) {
	m.mu.Lock()
	if len(m.pending) > 0 {
		m.pending = m.pending[1:]
	}
	m.current = job
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := m.sqlite.MarkPipelineRunStarted(ctx, job.id, time.Now().UTC()); err != nil {
		m.log.Error("failed to mark pipeline run started", "id", job.id, "error", err)
	}
}

func (m *Manager) runJob(job *pipelineJob) {
	ctx := context.Background()
	start := time.Now()
	result := m.runner.Run(ctx)
	finish := time.Now()
	duration := finish.Sub(start)
	m.persistResult(job, finish, duration, result)
}

func (m *Manager) persistResult(job *pipelineJob, finishedAt time.Time, duration time.Duration, result PipelineExecutionResult) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	status := "success"
	var errMsgPtr *string
	var durationPtr *int64
	if result.Err != nil {
		status = "failed"
		msg := result.ErrorMessage
		if msg == "" {
			msg = result.Err.Error()
		}
		trimmed := truncateMessage(msg)
		errMsgPtr = &trimmed
	}
	if duration > 0 {
		dur := int64(duration.Seconds())
		durationPtr = &dur
	}
	if err := m.sqlite.CompletePipelineRun(ctx, job.id, finishedAt.UTC(), status, durationPtr, errMsgPtr, result.RecordsPicking, result.RecordsPacking); err != nil {
		m.log.Error("failed to record pipeline run completion", "id", job.id, "status", status, "error", err)
	}
}

func (m *Manager) finishJob() {
	m.mu.Lock()
	m.current = nil
	m.mu.Unlock()
}

func truncateMessage(msg string) string {
	if len(msg) <= 512 {
		return msg
	}
	return msg[:512]
}

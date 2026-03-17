package orchestrator

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
)

// CommandRunner executes the pipeline by invoking a shell command (e.g., `go run .`).
type CommandRunner struct {
	WorkDir string
	Log     *slog.Logger
}

// Run executes the configured command and captures stdout/stderr for diagnostics.
func (r *CommandRunner) Run(ctx context.Context) PipelineExecutionResult {
	cmd := exec.CommandContext(ctx, "go", "run", ".")
	cmd.Dir = r.WorkDir
	cmd.Env = os.Environ()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	start := time.Now()
	err := cmd.Run()
	duration := time.Since(start)
	if err != nil {
		r.Log.Error("pipeline execution failed", "error", err)
	}
	message := strings.TrimSpace(stderr.String())
	if message == "" {
		message = strings.TrimSpace(stdout.String())
	}
	return PipelineExecutionResult{
		Duration:     duration,
		Err:          err,
		ErrorMessage: message,
	}
}

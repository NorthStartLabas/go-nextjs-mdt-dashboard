export type PipelineRun = {
	id: number;
	status: "queued" | "running" | "success" | "failed";
	queued_at: string;
	started_at?: string;
	finished_at?: string;
	duration_seconds?: number;
	error_message?: string;
	records_picking?: number;
	records_packing?: number;
};

export type PipelineStats = {
	success_count: number;
	failure_count: number;
	avg_success_duration_seconds?: number;
	avg_failure_duration_seconds?: number;
};

export type PipelineStatusResponse = {
	active_run?: PipelineRun;
	queued_runs: PipelineRun[];
	recent_runs: PipelineRun[];
	stats: PipelineStats;
	pending_jobs: number;
	pending_limit: number;
};

export type PipelineHistoryResponse = {
	days: number;
	runs: PipelineRun[];
};

export type QueueEnqueueResponse = {
	run: PipelineRun;
	jobs_ahead: number;
	pending_limit: number;
};

export type ConfigName = "breaks" | "floor" | "operator";

export type HealthResponse = {
	server_start: string;
	uptime_seconds: number;
	hostname: string;
	go_version: string;
	queue_depth: number;
	db_status: string;
};

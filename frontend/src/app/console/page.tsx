import type { ReactNode } from "react";

import {
	getHealth,
	getPipelineHistory,
	getPipelineStatus,
} from "@/lib/api-client";
import { buildStatusSeries } from "@/lib/pipeline-insights";
import { RunPipelineButton } from "@/components/console/run-pipeline-button";
import { PipelineSuccessChart } from "@/components/console/pipeline-success-chart";
import type { PipelineRun } from "@/types/pipeline";

export default async function ConsoleHome() {
	const [status, history, health] = await Promise.all([
		getPipelineStatus(),
		getPipelineHistory(30),
		getHealth(),
	]);

	const totalRuns = status.stats.success_count + status.stats.failure_count;
	const successRate = totalRuns
		? Math.round((status.stats.success_count / totalRuns) * 100)
		: 100;
	const avgSuccessDuration = formatDuration(
		status.stats.avg_success_duration_seconds,
	);
	const chartData = buildStatusSeries(history.runs);
	const recentRuns = status.recent_runs.slice(0, 6);

	return (
		<div className="space-y-10">
			<header className="flex flex-col gap-4 rounded-3xl border border-white/5 bg-gradient-to-r from-slate-900 to-slate-950 p-8 shadow-inner shadow-white/5 lg:flex-row lg:items-center lg:justify-between">
				<div>
					<p className="text-xs uppercase tracking-[0.5em] text-white/50">
						Pipelined Productivity
					</p>
					<h1 className="text-4xl font-semibold text-white">
						Command Console
					</h1>
					<p className="mt-2 max-w-2xl text-sm text-white/60">
						Monitor extraction velocity, launch orchestrations on demand, and keep auxiliary mappings aligned with the floor.
					</p>
				</div>
				<RunPipelineButton />
			</header>

			<section className="grid gap-6 lg:grid-cols-4">
				<Card>
					<p className="card-label">Live status</p>
					<h3 className="text-2xl font-semibold text-white">
						{activeHeadline(status.active_run)}
					</h3>
					<p className="text-sm text-white/50">
						{status.active_run
							? `Started ${formatRelative(status.active_run.started_at)}`
							: "Orchestrator is idle"}
					</p>
				</Card>
				<Card>
					<p className="card-label">Queue depth</p>
					<h3 className="text-4xl font-semibold text-white">
						{status.pending_jobs}
					</h3>
					<p className="text-sm text-white/50">
						Capacity {status.pending_jobs}/{status.pending_limit}
					</p>
				</Card>
				<Card>
					<p className="card-label">30-day success rate</p>
					<h3 className="text-4xl font-semibold text-white">
						{successRate}%
					</h3>
					<p className="text-sm text-white/50">
						{status.stats.success_count} passes / {status.stats.failure_count} fails
					</p>
				</Card>
				<Card>
					<p className="card-label">Avg success duration</p>
					<h3 className="text-3xl font-semibold text-white">{avgSuccessDuration}</h3>
					<p className="text-sm text-white/50">Based on recent runs</p>
				</Card>
			</section>

			<section className="grid gap-8 lg:grid-cols-3">
				<div className="lg:col-span-2">
					<PipelineSuccessChart data={chartData} />
				</div>
				<div className="rounded-3xl border border-white/5 bg-slate-900/60 p-6">
					<p className="card-label">Machine health</p>
					<h3 className="text-3xl font-semibold text-white">
						{formatDurationHours(health.uptime_seconds)} uptime
					</h3>
					<ul className="mt-4 space-y-2 text-sm text-white/60">
						<li>Host: {health.hostname}</li>
						<li>Go runtime: {health.go_version}</li>
						<li>DB status: {health.db_status}</li>
					</ul>
				</div>
			</section>

			<section className="grid gap-8 lg:grid-cols-2">
				<div className="rounded-3xl border border-white/5 bg-slate-900/60 p-6">
					<div className="flex items-center justify-between">
						<div>
							<p className="card-label">Queue</p>
							<h3 className="text-2xl font-semibold text-white">Upcoming runs</h3>
						</div>
						<span className="text-sm text-white/50">{status.queued_runs.length || "None"}</span>
					</div>
					<div className="mt-4 space-y-3">
						{status.queued_runs.length === 0 && (
							<p className="text-sm text-white/50">No jobs waiting.</p>
						)}
						{status.queued_runs.map((run) => (
							<div
								key={run.id}
								className="rounded-2xl border border-cyan-500/20 bg-cyan-500/5 px-4 py-3"
							>
								<p className="text-sm font-semibold text-white">Run #{run.id}</p>
								<p className="text-xs text-white/60">
									Queued {formatRelative(run.queued_at)}
								</p>
							</div>
						))}
					</div>
				</div>
				<div className="rounded-3xl border border-white/5 bg-slate-900/60 p-6">
					<div className="flex items-center justify-between">
						<div>
							<p className="card-label">Recent activity</p>
							<h3 className="text-2xl font-semibold text-white">Latest runs</h3>
						</div>
						<span className="text-sm text-white/50">Last {recentRuns.length}</span>
					</div>
					<div className="mt-4 divide-y divide-white/5">
						{recentRuns.map((run) => (
							<div key={run.id} className="flex items-center justify-between py-3">
								<div>
									<p className="text-sm font-semibold text-white">#{run.id}</p>
									<p className="text-xs text-white/50">
										{run.status === "success" ? "Completed" : run.status} · {formatRelative(run.finished_at || run.started_at || run.queued_at)}
									</p>
								</div>
								<p className={`text-sm font-semibold ${run.status === "failed" ? "text-rose-300" : "text-emerald-300"}`}>
									{run.status}
								</p>
							</div>
						))}
					</div>
				</div>
			</section>
		</div>
	);
}

function Card({ children }: { children: ReactNode }) {
	return (
		<div className="rounded-3xl border border-white/5 bg-slate-900/60 p-6 shadow-inner shadow-white/5">
			{children}
		</div>
	);
}

function activeHeadline(run?: PipelineRun) {
	if (!run) return "Idle";
	return `Run #${run.id} in progress`;
}

function formatRelative(iso?: string) {
	if (!iso) return "—";
	const diffMs = new Date(iso).getTime() - Date.now();
	const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
	const minutes = Math.round(diffMs / 60000);
	if (Math.abs(minutes) < 90) {
		return rtf.format(minutes, "minute");
	}
	const hours = Math.round(minutes / 60);
	if (Math.abs(hours) < 72) {
		return rtf.format(hours, "hour");
	}
	const days = Math.round(hours / 24);
	return rtf.format(days, "day");
}

function formatDuration(seconds?: number) {
	if (!seconds) return "—";
	const mins = Math.floor(seconds / 60);
	const secs = Math.round(seconds % 60);
	return `${mins}m ${secs}s`;
}

function formatDurationHours(seconds: number) {
	const hours = seconds / 3600;
	if (hours < 1) {
		return `${Math.round(seconds / 60)} min`;
	}
	return `${hours.toFixed(1)} h`;
}

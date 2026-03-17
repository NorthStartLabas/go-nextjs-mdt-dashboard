import type { PipelineRun } from "@/types/pipeline";

export function buildStatusSeries(runs: PipelineRun[]) {
	const buckets = groupByDay(runs, (bucket, run) => {
		if (run.status === "failed") {
			bucket.failed += 1;
		} else if (run.status === "success") {
			bucket.success += 1;
		}
	});
	return buckets.map(({ day, payload }) => ({
		label: formatDay(day),
		...payload,
	}));
}

export function buildDurationSeries(runs: PipelineRun[]) {
	const buckets = groupByDay(runs, (bucket, run) => {
		if (run.status !== "success" || !run.duration_seconds) {
			return;
		}
		bucket.durationSum += run.duration_seconds;
		bucket.count += 1;
	});
	return buckets.map(({ day, payload }) => ({
		label: formatDay(day),
		avgDuration: payload.count ? payload.durationSum / payload.count : 0,
	}));
}

export function summarizeFailures(runs: PipelineRun[]) {
	const map = new Map<string, number>();
	runs.forEach((run) => {
		if (run.status !== "failed" || !run.error_message) return;
		map.set(run.error_message, (map.get(run.error_message) || 0) + 1);
	});
	return Array.from(map.entries())
		.sort((a, b) => b[1] - a[1])
		.map(([message, count]) => ({ message, count }));
}

type ReducerPayload = {
	success: number;
	failed: number;
	durationSum: number;
	count: number;
};

type Reducer = (bucket: ReducerPayload, run: PipelineRun) => void;

function groupByDay(runs: PipelineRun[], reducer: Reducer) {
	const map = new Map<string, ReducerPayload>();
	runs.forEach((run) => {
		const key = (run.finished_at || run.queued_at).slice(0, 10);
		const bucket =
			map.get(key) || {
				success: 0,
				failed: 0,
				durationSum: 0,
				count: 0,
			};
		reducer(bucket, run);
		map.set(key, bucket);
	});
	return Array.from(map.entries())
		.sort(([a], [b]) => (a > b ? 1 : -1))
		.slice(-12)
		.map(([day, payload]) => ({ day, payload }));
}

function formatDay(dateStr: string) {
	return new Intl.DateTimeFormat("en", {
		month: "short",
		day: "numeric",
	}).format(new Date(dateStr));
}

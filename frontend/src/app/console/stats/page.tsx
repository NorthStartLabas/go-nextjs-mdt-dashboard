import {
	getPipelineHistory,
	getPipelineStatus,
} from "@/lib/api-client";
import {
	buildDurationSeries,
	buildStatusSeries,
	summarizeFailures,
} from "@/lib/pipeline-insights";
import { DurationChart } from "@/components/console/duration-chart";
import { PipelineSuccessChart } from "@/components/console/pipeline-success-chart";

export default async function StatsPage() {
	const [history, status] = await Promise.all([
		getPipelineHistory(30),
		getPipelineStatus(),
	]);
	const statusSeries = buildStatusSeries(history.runs);
	const durationSeries = buildDurationSeries(history.runs);
	const failures = summarizeFailures(history.runs).slice(0, 5);

	return (
		<div className="space-y-8">
			<header>
				<p className="text-xs uppercase tracking-[0.5em] text-white/40">
					Analytics
				</p>
				<h1 className="text-3xl font-semibold text-white">Performance intelligence</h1>
				<p className="text-sm text-white/50">Fresh numbers straight from SQLite.</p>
			</header>
			<section className="grid gap-6 lg:grid-cols-3">
				<PipelineSuccessChart data={statusSeries} />
				<DurationChart data={durationSeries} />
				<div className="rounded-3xl border border-white/5 bg-slate-900/60 p-6">
					<p className="card-label">Failure clusters</p>
					<ul className="mt-4 space-y-4 text-sm">
						{failures.length === 0 && (
							<li className="text-white/50">Zero recorded failures this month — keep it up.</li>
						)}
						{failures.map((item) => (
							<li key={item.message} className="rounded-2xl border border-rose-400/10 bg-rose-50/5 p-4">
								<p className="font-semibold text-rose-100">{item.message}</p>
								<p className="text-xs text-rose-200">{item.count} occurrences</p>
							</li>
						))}
					</ul>
				</div>
			</section>
			<section className="grid gap-6 lg:grid-cols-4">
				<MetricCard title="Success count" value={status.stats.success_count} tone="emerald" />
				<MetricCard title="Fail count" value={status.stats.failure_count} tone="rose" />
				<MetricCard
					title="Avg fail duration"
					value={formatSeconds(status.stats.avg_failure_duration_seconds)}
					tone="amber"
				/>
				<MetricCard
					title="Avg success duration"
					value={formatSeconds(status.stats.avg_success_duration_seconds)}
					tone="sky"
				/>
			</section>
		</div>
	);
}

type Tone = "emerald" | "rose" | "amber" | "sky";

function MetricCard({
	title,
	value,
	tone,
}: {
	title: string;
	value: number | string;
	tone: Tone;
}) {
	const palette: Record<Tone, string> = {
		emerald: "from-emerald-500/20",
		rose: "from-rose-500/20",
		amber: "from-amber-400/20",
		sky: "from-sky-400/20",
	};
	return (
		<div className={`rounded-3xl border border-white/5 bg-gradient-to-br ${palette[tone]} to-transparent p-6`}>
			<p className="card-label">{title}</p>
			<h3 className="text-3xl font-semibold text-white">{value}</h3>
		</div>
	);
}

function formatSeconds(value?: number) {
	if (!value) return "—";
	return `${value.toFixed(1)}s`;
}

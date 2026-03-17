import { getPipelineHistory } from "@/lib/api-client";

export default async function LogsPage() {
	const history = await getPipelineHistory(30);
	return (
		<div className="space-y-6">
			<header>
				<p className="text-xs uppercase tracking-[0.5em] text-white/40">
					Traceability
				</p>
				<h1 className="text-3xl font-semibold text-white">Pipeline logbook</h1>
				<p className="text-sm text-white/50">
					Last {history.runs.length} runs across {history.days} days.
				</p>
			</header>
			<div className="overflow-hidden rounded-3xl border border-white/5">
				<table className="min-w-full divide-y divide-white/5 text-sm">
					<thead className="bg-white/5 text-left text-xs uppercase tracking-widest text-white/50">
						<tr>
							<th className="px-6 py-4">Run</th>
							<th className="px-6 py-4">Status</th>
							<th className="px-6 py-4">Queued</th>
							<th className="px-6 py-4">Duration</th>
							<th className="px-6 py-4">Notes</th>
						</tr>
					</thead>
					<tbody className="divide-y divide-white/5 bg-slate-950/50">
						{history.runs.map((run) => (
							<tr key={run.id} className="hover:bg-white/5">
								<td className="px-6 py-4 font-semibold text-white">#{run.id}</td>
								<td className="px-6 py-4">
									<span className={`rounded-full px-2.5 py-1 text-xs font-semibold ${badgeColor(run.status)}`}>
										{run.status}
									</span>
								</td>
								<td className="px-6 py-4 text-white/60">
									{formatDate(run.queued_at)}
								</td>
								<td className="px-6 py-4 text-white/60">
									{run.duration_seconds ? formatDuration(run.duration_seconds) : "—"}
								</td>
								<td className="px-6 py-4 text-white/60">
									{run.error_message || "—"}
								</td>
							</tr>
						))}
					</tbody>
				</table>
			</div>
		</div>
	);
}

function formatDate(input: string) {
	return new Intl.DateTimeFormat("en", {
		year: "numeric",
		month: "short",
		day: "numeric",
		hour: "numeric",
		minute: "2-digit",
	}).format(new Date(input));
}

function formatDuration(seconds: number) {
	const mins = Math.floor(seconds / 60);
	const secs = seconds % 60;
	return `${mins}m ${secs}s`;
}

function badgeColor(status: string) {
	switch (status) {
		case "success":
			return "bg-emerald-400/20 text-emerald-200";
		case "failed":
			return "bg-rose-400/20 text-rose-200";
		case "running":
			return "bg-cyan-400/20 text-cyan-200";
		default:
			return "bg-white/10 text-white";
	}
}

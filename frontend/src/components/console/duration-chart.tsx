"use client";

import {
	Line,
	LineChart,
	ResponsiveContainer,
	Tooltip,
	XAxis,
} from "recharts";

type DurationPoint = {
	label: string;
	avgDuration: number;
};

export function DurationChart({ data }: { data: DurationPoint[] }) {
	return (
		<div className="h-72 w-full rounded-3xl border border-white/5 bg-slate-900/60 p-4">
			<h3 className="text-sm uppercase tracking-[0.3em] text-white/50">
				Cycle time
			</h3>
			<p className="text-2xl font-semibold text-white">Avg duration</p>
			<div className="mt-4 h-48">
				<ResponsiveContainer width="100%" height="100%">
					<LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
						<XAxis dataKey="label" stroke="#94a3b8" axisLine={false} tickLine={false} />
					<Tooltip
						formatter={(value) => {
							const numeric = Number(value ?? 0);
							return [`${numeric.toFixed(1)}s`, "Avg"] as [string, string];
						}}
						contentStyle={{ background: "#020617", borderRadius: 16, border: "1px solid rgba(255,255,255,0.08)" }}
					/>
						<Line
							type="monotone"
							dataKey="avgDuration"
							stroke="#a78bfa"
							strokeWidth={3}
							dot={false}
						/>
					</LineChart>
				</ResponsiveContainer>
			</div>
		</div>
	);
}

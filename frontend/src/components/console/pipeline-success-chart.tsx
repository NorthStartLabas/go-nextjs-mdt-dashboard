"use client";

import {
	Area,
	AreaChart,
	ResponsiveContainer,
	Tooltip,
	XAxis,
} from "recharts";

type ChartPoint = {
	label: string;
	success: number;
	failed: number;
};

export function PipelineSuccessChart({ data }: { data: ChartPoint[] }) {
	return (
		<div className="h-72 w-full rounded-3xl border border-white/5 bg-slate-900/60 p-4">
			<h3 className="text-sm uppercase tracking-[0.3em] text-white/50">
				Run cadence
			</h3>
			<p className="text-2xl font-semibold text-white">Success vs failure</p>
			<div className="mt-4 h-48">
				<ResponsiveContainer width="100%" height="100%">
					<AreaChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
						<XAxis dataKey="label" stroke="#94a3b8" axisLine={false} tickLine={false} />
						<Tooltip
							contentStyle={{ background: "#020617", borderRadius: 16, border: "1px solid rgba(255,255,255,0.08)" }}
						/>
						<Area
							type="monotone"
							dataKey="success"
							stackId="1"
							stroke="#34d399"
							fill="url(#successGradient)"
							strokeWidth={2}
						/>
						<Area
							type="monotone"
							dataKey="failed"
							stackId="1"
							stroke="#f87171"
							fill="url(#failedGradient)"
							strokeWidth={2}
						/>
						<defs>
							<linearGradient id="successGradient" x1="0" y1="0" x2="0" y2="1">
								<stop offset="5%" stopColor="#34d399" stopOpacity={0.8} />
								<stop offset="95%" stopColor="#34d399" stopOpacity={0.05} />
							</linearGradient>
							<linearGradient id="failedGradient" x1="0" y1="0" x2="0" y2="1">
								<stop offset="5%" stopColor="#f87171" stopOpacity={0.7} />
								<stop offset="95%" stopColor="#f87171" stopOpacity={0.05} />
							</linearGradient>
						</defs>
					</AreaChart>
				</ResponsiveContainer>
			</div>
		</div>
	);
}

"use client";

import { useState, useTransition } from "react";

import { Rocket } from "lucide-react";

import { runPipelineAction } from "@/app/console/server-actions";

export function RunPipelineButton() {
	const [pending, startTransition] = useTransition();
	const [message, setMessage] = useState<string | null>(null);
	const triggerRun = () => {
		startTransition(() => {
			runPipelineAction()
				.then((result) => {
					setMessage(result.message);
				})
				.catch((error) => {
					setMessage(error.message ?? "Unable to trigger pipeline.");
				});
		});
	};
	return (
		<div>
			<button
				onClick={triggerRun}
				disabled={pending}
				className="flex items-center gap-3 rounded-2xl bg-gradient-to-r from-emerald-400 via-cyan-400 to-blue-500 px-6 py-3 text-lg font-semibold text-slate-900 shadow-xl shadow-emerald-500/30 transition hover:scale-[1.01] disabled:opacity-60"
			>
				<Rocket className="h-5 w-5" />
				{pending ? "Queueing..." : "Run pipeline"}
			</button>
			{message && (
				<p className="mt-2 text-sm text-slate-400">{message}</p>
			)}
		</div>
	);
}

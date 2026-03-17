"use client";

import { useState, useTransition } from "react";

import type { ConfigName } from "@/types/pipeline";

import { updateConfigAction } from "@/app/console/server-actions";

type Props = {
	name: ConfigName;
	title: string;
	description: string;
	initialValue: string;
};

export function ConfigEditor({ name, title, description, initialValue }: Props) {
	const [value, setValue] = useState(initialValue);
	const [pending, startTransition] = useTransition();
	const [feedback, setFeedback] = useState<string>("");

	const save = () => {
		startTransition(() => {
			updateConfigAction(name, value)
				.then(() => setFeedback("Saved"))
				.catch((error) => setFeedback(error.message ?? "Unable to save"));
		});
	};

	return (
		<div className="rounded-3xl border border-white/5 bg-slate-900/60 p-6">
			<div className="flex items-start justify-between gap-4">
				<div>
					<p className="text-xs uppercase tracking-[0.5em] text-white/40">{name}</p>
					<h3 className="text-2xl font-semibold text-white">{title}</h3>
					<p className="text-sm text-white/50">{description}</p>
				</div>
				<div className="flex gap-2">
					<button
						onClick={() => setValue(initialValue)}
						disabled={pending}
						className="rounded-xl border border-white/10 px-4 py-2 text-sm text-white/70 hover:border-white/30"
					>
						Reset
					</button>
					<button
						onClick={save}
						disabled={pending}
						className="rounded-xl bg-emerald-400/90 px-4 py-2 text-sm font-semibold text-slate-900 shadow hover:bg-emerald-300 disabled:opacity-60"
					>
						{pending ? "Saving..." : "Save"}
					</button>
				</div>
			</div>
			<textarea
				value={value}
				onChange={(event) => setValue(event.target.value)}
				spellCheck={false}
				className="mt-4 h-64 w-full rounded-2xl border border-white/10 bg-black/30 font-mono text-sm text-white focus:border-emerald-400/60"
			/>
			{feedback && <p className="mt-2 text-xs text-emerald-300">{feedback}</p>}
		</div>
	);
}

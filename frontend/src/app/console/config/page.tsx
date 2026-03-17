import { ConfigEditor } from "@/components/console/config-editor";
import { fetchConfig } from "@/lib/api-client";

const sections = [
	{
		name: "breaks" as const,
		title: "Break allowances",
		description: "LGNUM→hour adjustments feeding productivity math.",
	},
	{
		name: "floor" as const,
		title: "Floor mapping",
		description: "Route aliases to floor semantics (second_floor, mezzanine, etc).",
	},
	{
		name: "operator" as const,
		title: "Operator mapping",
		description: "Badge IDs to friendly names for reporting.",
	},
];

export default async function ConfigPage() {
	const configs = await Promise.all(
		sections.map(async (section) => {
			const content = await fetchConfig(section.name);
			return JSON.stringify(content, null, 2);
		}),
	);

	return (
		<div className="space-y-6">
			<header>
				<p className="text-xs uppercase tracking-[0.5em] text-white/40">Source of truth</p>
				<h1 className="text-3xl font-semibold text-white">Configuration studio</h1>
				<p className="text-sm text-white/50">
					Edits land directly on the shared JSON files powering the Go orchestrator.
				</p>
			</header>
			<div className="space-y-6">
				{sections.map((section, idx) => (
					<ConfigEditor
						key={section.name}
						name={section.name}
						title={section.title}
						description={section.description}
						initialValue={configs[idx]}
					/>
				))}
			</div>
		</div>
	);
}

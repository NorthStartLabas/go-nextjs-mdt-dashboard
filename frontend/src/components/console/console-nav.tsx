"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
	Gauge,
	LineChart,
	LogOut,
	ScrollText,
	Settings2,
} from "lucide-react";

type NavLink = {
	href: string;
	label: string;
	icon: "gauge" | "chart" | "log" | "settings";
};

const icons = {
	gauge: Gauge,
	chart: LineChart,
	log: ScrollText,
	settings: Settings2,
};

export function ConsoleNav({
	links,
	onSignOut,
}: {
	links: NavLink[];
	onSignOut: () => Promise<void>;
}) {
	const pathname = usePathname();
	return (
		<aside className="hidden min-h-screen w-64 flex-col border-r border-white/5 bg-gradient-to-b from-slate-900 via-slate-950 to-black px-6 py-8 lg:flex">
			<div>
				<p className="text-xs uppercase tracking-[0.5em] text-white/50">
					Atlas
				</p>
				<h2 className="mt-2 text-2xl font-semibold tracking-tight text-white">
					Console
				</h2>
				<p className="text-sm text-white/50">Productivity orchestration</p>
			</div>
			<nav className="mt-10 space-y-2">
				{links.map((link) => {
					const Icon = icons[link.icon];
					const active = pathname.startsWith(link.href);
					return (
						<Link
							key={link.href}
							href={link.href}
							className={`flex items-center gap-3 rounded-xl px-4 py-3 text-sm font-medium transition ${
								active
									? "bg-white/10 text-white"
									: "text-white/60 hover:bg-white/5 hover:text-white"
							}`}
						>
							<Icon className="h-4 w-4" />
							{link.label}
						</Link>
					);
				})}
			</nav>
			<div className="mt-auto">
				<form action={onSignOut}>
					<button
						type="submit"
						className="flex w-full items-center justify-center gap-2 rounded-xl border border-white/10 px-4 py-3 text-sm font-semibold text-white transition hover:border-white/30"
					>
						<LogOut className="h-4 w-4" />
						Sign out
					</button>
				</form>
			</div>
		</aside>
	);
}

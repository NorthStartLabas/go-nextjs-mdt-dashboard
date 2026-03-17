import type { ReactNode } from "react";

import { ConsoleNav } from "@/components/console/console-nav";

import { logoutAction } from "./server-actions";

import { requireAuth } from "@/lib/auth";

const navLinks = [
	{ href: "/console", label: "Overview", icon: "gauge" as const },
	{ href: "/console/stats", label: "Stats", icon: "chart" as const },
	{ href: "/console/logs", label: "Logs", icon: "log" as const },
	{ href: "/console/config", label: "Config", icon: "settings" as const },
];

export default async function ConsoleLayout({
	children,
}: {
	children: ReactNode;
}) {
	await requireAuth();
	return (
		<div className="flex min-h-screen bg-slate-950 text-slate-100">
			<ConsoleNav links={navLinks} onSignOut={logoutAction} />
			<main className="flex-1 overflow-y-auto px-8 py-10">
				{children}
			</main>
		</div>
	);
}

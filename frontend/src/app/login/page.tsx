import type { Metadata } from "next";

import { LoginForm } from "@/components/login/login-form";

import { redirectIfAuthenticated } from "@/lib/auth";

export const metadata: Metadata = {
	title: "Pipeline Console — Login",
	description: "Secure gateway into the productivity command center.",
};

export default async function LoginPage() {
	await redirectIfAuthenticated();
	return (
		<div className="relative min-h-screen overflow-hidden bg-slate-950 text-white">
			<div className="absolute inset-0">
				<div className="absolute -left-32 top-8 h-72 w-72 rounded-full bg-cyan-500/30 blur-[120px]" />
				<div className="absolute right-0 bottom-0 h-80 w-80 rounded-full bg-amber-400/20 blur-[150px]" />
			</div>
			<div className="relative flex min-h-screen flex-col items-center justify-center px-6 py-20">
				<div className="mb-10 text-center">
					<p className="text-xs uppercase tracking-[0.4em] text-white/60">
						Extraction Control
					</p>
					<h1 className="mt-3 text-4xl font-semibold tracking-tight">
						Atlas Console Access
					</h1>
					<p className="mt-2 max-w-lg text-base text-white/60">
						Monitor throughput, trigger orchestrations, and edit mapping data — all from a single, opinionated cockpit.
					</p>
				</div>
				<LoginForm />
			</div>
		</div>
	);
}

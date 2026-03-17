"use client";

import { useFormState, useFormStatus } from "react-dom";

import { loginAction, type LoginState } from "@/app/login/actions";

const initialState: LoginState = {};

export function LoginForm() {
	const [state, formAction] = useFormState(loginAction, initialState);
	return (
		<form
			action={formAction}
			className="space-y-6 rounded-2xl bg-zinc-950/50 p-8 backdrop-blur border border-white/10 shadow-2xl"
		>
			<div>
				<label className="text-sm uppercase tracking-[0.3em] text-zinc-400">
					Username
				</label>
				<input
					name="username"
					type="text"
					required
					className="mt-2 w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-lg text-white placeholder:text-white/40 focus:border-cyan-400 focus:outline-none"
					placeholder="admin"
				/>
			</div>
			<div>
				<label className="text-sm uppercase tracking-[0.3em] text-zinc-400">
					Password
				</label>
				<input
					name="password"
					type="password"
					required
					className="mt-2 w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-lg text-white placeholder:text-white/40 focus:border-amber-400 focus:outline-none"
					placeholder="••••••••"
				/>
			</div>
			{state?.error && (
				<p className="text-sm text-rose-300">{state.error}</p>
			)}
			<SubmitButton />
		</form>
	);
}

function SubmitButton() {
	const { pending } = useFormStatus();
	return (
		<button
			type="submit"
			disabled={pending}
			className="flex w-full items-center justify-center rounded-xl bg-gradient-to-r from-cyan-400 to-blue-600 py-3 text-lg font-semibold text-white shadow-lg shadow-cyan-500/30 transition hover:from-cyan-300 hover:to-blue-500 disabled:opacity-60"
		>
			{pending ? "Verifying..." : "Enter Console"}
		</button>
	);
}

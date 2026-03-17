"use server";

import { redirect } from "next/navigation";

import { establishSession, validateCredentials } from "@/lib/auth";

export type LoginState = {
	error?: string;
};

export async function loginAction(
	prevState: LoginState,
	formData: FormData,
): Promise<LoginState> {
	const username = String(formData.get("username") ?? "").trim();
	const password = String(formData.get("password") ?? "");
	if (!username || !password) {
		return { error: "Please provide both username and password." };
	}
	if (!validateCredentials(username, password)) {
		return { error: "Invalid credentials." };
	}
	establishSession();
	redirect("/console");
}

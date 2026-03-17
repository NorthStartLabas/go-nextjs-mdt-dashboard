import crypto from "crypto";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";

import { env } from "@/env";

const SESSION_COOKIE = "console_session";
const SESSION_MAX_AGE = 60 * 60 * 12; // 12 hours

const secureCookie = process.env.NODE_ENV === "production";

function buildSessionToken() {
	return crypto
		.createHmac("sha256", env.SESSION_SECRET)
		.update(`${env.ADMIN_USERNAME}:${env.ADMIN_PASSWORD}`)
		.digest("hex");
}

export function validateCredentials(username: string, password: string) {
	return (
		username === env.ADMIN_USERNAME &&
		password === env.ADMIN_PASSWORD
	);
}

export async function establishSession() {
	const store = await cookies();
	store.set(SESSION_COOKIE, buildSessionToken(), {
		httpOnly: true,
		sameSite: "lax",
		secure: secureCookie,
		maxAge: SESSION_MAX_AGE,
		path: "/",
	});
}

export async function destroySession() {
	const store = await cookies();
	store.delete(SESSION_COOKIE);
}

export async function isAuthenticated() {
	const store = await cookies();
	const token = store.get(SESSION_COOKIE)?.value;
	return token === buildSessionToken();
}

export async function requireAuth() {
	if (!(await isAuthenticated())) {
		redirect("/login");
	}
}

export async function redirectIfAuthenticated() {
	if (await isAuthenticated()) {
		redirect("/console");
	}
}

import "server-only";

import { env } from "@/env";
import {
	ConfigName,
	HealthResponse,
	PipelineHistoryResponse,
	PipelineStatusResponse,
	QueueEnqueueResponse,
} from "@/types/pipeline";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
	const response = await fetch(`${env.API_BASE_URL}${path}`, {
		...init,
		headers: {
			"Content-Type": "application/json",
			...(init?.headers as Record<string, string> | undefined),
		},
		cache: "no-store",
	})
		.catch((err) => {
			throw new Error(`failed to reach orchestrator API: ${err.message}`);
		});
	if (!response.ok) {
		let details = response.statusText;
		try {
			const body = await response.json();
			details = body?.error ?? details;
		} catch {
			// ignore json parse errors
		}
		throw new Error(`api error (${response.status}): ${details}`);
	}
	return (await response.json()) as T;
}

export function getPipelineStatus() {
	return apiFetch<PipelineStatusResponse>("/api/pipeline/status");
}

export function getPipelineHistory(days: number) {
	const url = `/api/pipeline/history?days=${days}`;
	return apiFetch<PipelineHistoryResponse>(url);
}

export function runPipeline() {
	return apiFetch<QueueEnqueueResponse>("/api/pipeline/run", {
		method: "POST",
	});
}

export function fetchConfig(name: ConfigName) {
	return apiFetch<Record<string, unknown>>(`/api/config/${name}`);
}

export function updateConfig(name: ConfigName, payload: unknown) {
	return apiFetch<Record<string, unknown>>(`/api/config/${name}`, {
		method: "PUT",
		body: JSON.stringify(payload),
	});
}

export function getHealth() {
	return apiFetch<HealthResponse>("/api/health");
}

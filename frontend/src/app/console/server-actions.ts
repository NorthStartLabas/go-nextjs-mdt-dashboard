"use server";

import { revalidatePath } from "next/cache";
import { redirect } from "next/navigation";

import { destroySession, requireAuth } from "@/lib/auth";
import { runPipeline, updateConfig } from "@/lib/api-client";
import type { ConfigName } from "@/types/pipeline";
import { QueueEnqueueResponse } from "@/types/pipeline";

type ActionResponse = {
	success: boolean;
	message: string;
};

export async function runPipelineAction(): Promise<ActionResponse> {
	await requireAuth();
	const response = await runPipeline();
	revalidatePath("/console");
	revalidatePath("/console/logs");
	return {
		success: true,
		message: buildQueueMessage(response),
	};
}

function buildQueueMessage(result: QueueEnqueueResponse) {
	if (result.jobs_ahead === 0) {
		return "Pipeline scheduled and will start momentarily.";
	}
	return `Pipeline enqueued. ${result.jobs_ahead} run${result.jobs_ahead > 1 ? "s" : ""} ahead.`;
}

export async function logoutAction() {
	await destroySession();
	redirect("/login");
}

export async function updateConfigAction(name: ConfigName, content: string) {
	await requireAuth();
	let parsed: unknown;
	try {
		parsed = JSON.parse(content);
	} catch {
		throw new Error("Config must be valid JSON");
	}
	await updateConfig(name, parsed);
	revalidatePath("/console/config");
}

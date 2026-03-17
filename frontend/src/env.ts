import { z } from "zod";

const serverSchema = z.object({
	ADMIN_USERNAME: z.string().min(1, "ADMIN_USERNAME is required"),
	ADMIN_PASSWORD: z.string().min(1, "ADMIN_PASSWORD is required"),
	SESSION_SECRET: z.string().min(16, "SESSION_SECRET must be at least 16 characters"),
	API_BASE_URL: z
		.string()
		.url("API_BASE_URL must be a valid URL, e.g., http://localhost:8080"),
});

export const env = serverSchema.parse({
	ADMIN_USERNAME: process.env.ADMIN_USERNAME,
	ADMIN_PASSWORD: process.env.ADMIN_PASSWORD,
	SESSION_SECRET: process.env.SESSION_SECRET,
	API_BASE_URL: process.env.API_BASE_URL,
});

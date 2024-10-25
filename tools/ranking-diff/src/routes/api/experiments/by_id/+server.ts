import type { RequestEvent } from '@sveltejs/kit';
import { experimentById } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
	const {
		id
	}: {
		id: number;
	} = await request.json();
	const experiment = experimentById(id);

	return new Response(JSON.stringify(experiment));
}

import type { RequestEvent } from '@sveltejs/kit';
import { unlike } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
	const {
		baselineId,
		experimentId,
		queryId
	}: {
		baselineId: number;
		experimentId: number;
		queryId: number;
	} = await request.json();

	unlike(baselineId, experimentId, queryId);

	return new Response('OK');
}

import type { RequestEvent } from '@sveltejs/kit';
import { likedState } from '$lib/db';

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

	const res = likedState(baselineId, experimentId, queryId);

	return new Response(JSON.stringify(res));
}

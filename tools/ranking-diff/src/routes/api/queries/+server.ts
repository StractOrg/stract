import type { RequestEvent } from '@sveltejs/kit';
import { getQueries } from '$lib/db';

export async function GET({}: RequestEvent): Promise<Response> {
	const queries = getQueries();

	return new Response(JSON.stringify(queries));
}

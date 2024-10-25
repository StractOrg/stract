import type { RequestEvent } from '@sveltejs/kit';
import { insertQuery } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
	const {
		query
	}: {
		query: string;
	} = await request.json();

	insertQuery(query);

	return new Response('OK');
}

import type { RequestEvent } from '@sveltejs/kit';
import { getQueriesByCategory } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
	const {
		categoryId
	}: {
		categoryId: number;
	} = await request.json();

	const queries = getQueriesByCategory(categoryId);

	return new Response(JSON.stringify(queries));
}

import type { RequestEvent } from '@sveltejs/kit';
import { getCategories } from '$lib/db';

export async function GET({}: RequestEvent): Promise<Response> {
	const experiments = getCategories();

	return new Response(JSON.stringify(experiments));
}

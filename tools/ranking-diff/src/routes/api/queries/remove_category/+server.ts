import type { RequestEvent } from '@sveltejs/kit';
import { removeQueryCategories } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    queryId,
  }: {
    queryId: number;
  } = await request.json();
  removeQueryCategories(queryId);

  return new Response('OK');
}

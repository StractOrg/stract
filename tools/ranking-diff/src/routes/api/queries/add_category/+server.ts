import type { RequestEvent } from '@sveltejs/kit';
import { addQueryToCategory } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    categoryId,
    queryId,
  }: {
    categoryId: number;
    queryId: number;
  } = await request.json();
  addQueryToCategory(queryId, categoryId);

  return new Response('OK');
}

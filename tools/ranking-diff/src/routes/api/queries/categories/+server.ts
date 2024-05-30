import type { RequestEvent } from '@sveltejs/kit';
import { getQueryCategories } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    queryId,
  }: {
    queryId: number;
  } = await request.json();
  const res = getQueryCategories(queryId);

  return new Response(JSON.stringify(res));
}

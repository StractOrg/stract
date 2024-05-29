import type { RequestEvent } from '@sveltejs/kit';
import { isLiked } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    experimentId,
    queryId,
  }: {
    experimentId: number;
    queryId: number;
  } = await request.json();

  const res = isLiked(experimentId, queryId);

  return new Response(JSON.stringify(res));
}

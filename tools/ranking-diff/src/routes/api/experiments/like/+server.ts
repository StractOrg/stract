import type { RequestEvent } from '@sveltejs/kit';
import { like, type LikedState } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    baselineId,
    experimentId,
    queryId,
    state,
  }: {
    baselineId: number;
    experimentId: number;
    queryId: number;
    state: LikedState;
  } = await request.json();

  like(baselineId, experimentId, queryId, state);

  return new Response('OK');
}

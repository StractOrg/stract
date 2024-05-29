import type { RequestEvent } from '@sveltejs/kit';
import { unlike } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    experimentId,
    queryId,
  }: {
    experimentId: number;
    queryId: number;
  } = await request.json();

  unlike(experimentId, queryId);

  return new Response('OK');
}

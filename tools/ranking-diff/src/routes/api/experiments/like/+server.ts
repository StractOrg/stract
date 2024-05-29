import type { RequestEvent } from '@sveltejs/kit';
import { like } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    experimentId,
    queryId,
  }: {
    experimentId: number;
    queryId: number;
  } = await request.json();

  like(experimentId, queryId);

  return new Response('OK');
}

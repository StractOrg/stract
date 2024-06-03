import type { RequestEvent } from '@sveltejs/kit';
import { serpByQueryAndExperiment } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    experimentId,
    queryId,
  }: {
    experimentId: number;
    queryId: number;
  } = await request.json();

  const res = serpByQueryAndExperiment(queryId, experimentId);

  return new Response(JSON.stringify(res));
}

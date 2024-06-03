import type { RequestEvent } from '@sveltejs/kit';
import { queryIntersection } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    experimentA,
    experimentB,
  }: {
    experimentA: number;
    experimentB: number;
  } = await request.json();

  const queries = queryIntersection(experimentA, experimentB);

  return new Response(JSON.stringify(queries));
}

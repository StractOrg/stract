import type { RequestEvent } from '@sveltejs/kit';
import { addSerp } from '$lib/db';
import type { SimpleWebpage } from '$lib/webpage';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    experimentId,
    queryId,
    webpages,
  }: {
    experimentId: number;
    queryId: number;
    webpages: SimpleWebpage[];
  } = await request.json();

  addSerp(experimentId, queryId, webpages);

  return new Response('OK');
}

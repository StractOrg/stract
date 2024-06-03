import type { RequestEvent } from '@sveltejs/kit';
import { deleteExperiment } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    id,
  }: {
    id: number;
  } = await request.json();
  deleteExperiment(id);

  return new Response('OK');
}

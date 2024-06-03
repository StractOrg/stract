import type { RequestEvent } from '@sveltejs/kit';
import { renameExperiment } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    id,
    name,
  }: {
    id: number;
    name: string;
  } = await request.json();
  renameExperiment(id, name);

  return new Response('OK');
}

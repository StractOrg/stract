import type { RequestEvent } from '@sveltejs/kit';
import { queryById } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    id,
  }: {
    id: number;
  } = await request.json();
  const experiment = queryById(id);

  return new Response(JSON.stringify(experiment));
}

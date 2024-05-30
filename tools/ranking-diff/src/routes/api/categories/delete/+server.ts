import type { RequestEvent } from '@sveltejs/kit';
import { removeCategory } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    id,
  }: {
    id: number;
  } = await request.json();
  removeCategory(id);

  return new Response('OK');
}

import type { RequestEvent } from '@sveltejs/kit';
import { addCategory } from '$lib/db';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    name,
  }: {
    name: string;
  } = await request.json();

  addCategory(name);

  return new Response('OK');
}

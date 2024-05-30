import type { RequestEvent } from '@sveltejs/kit';
import { clearCategories } from '$lib/db';

export async function POST({}: RequestEvent): Promise<Response> {
  clearCategories();
  return new Response('OK');
}

import type { RequestEvent } from '@sveltejs/kit';
import { getExperiments } from '$lib/db';

export async function GET({}: RequestEvent): Promise<Response> {
  const experiments = getExperiments();

  return new Response(JSON.stringify(experiments));
}

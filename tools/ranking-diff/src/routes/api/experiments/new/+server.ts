import type { RequestEvent } from '@sveltejs/kit';
import { newExperiment } from '$lib/db';

export async function POST({}: RequestEvent): Promise<Response> {
  const curTime = new Date().toISOString();
  const experiment = newExperiment(curTime);

  return new Response(JSON.stringify(experiment));
}

import { fetchRemoteOptic } from '$lib/optics';
import { error, text, type RequestEvent } from '@sveltejs/kit';

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    opticUrl,
  }: {
    opticUrl: string;
  } = await request.json();

  try {
    const optic = await fetchRemoteOptic({
      opticUrl,
      fetch,
    });

    if (optic == undefined) {
      throw error(400, 'failed to fetch optic from url');
    }
  } catch (e) {
    if (e instanceof TypeError) {
      throw error(400, 'failed to fetch optic from url');
    }

    throw e;
  }

  // TODO: maybe validate optics syntax here?

  return text('success');
}

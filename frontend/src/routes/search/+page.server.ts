import { shouldShowCaptcha } from '$lib/captcha/rateLimiter';
import { extractSearchParams, type SearchParams } from '$lib/search';
import { redirect } from '@sveltejs/kit';
import type { Actions, PageServerLoadEvent } from './$types';

export const load = async ({ locals, getClientAddress, url, request }: PageServerLoadEvent) => {
  const searchParams: SearchParams | undefined =
    (locals['form'] && extractSearchParams(locals['form'])) || undefined;

  const clientAddress = request.headers.get('x-real-ip') || getClientAddress();

  if (await shouldShowCaptcha(clientAddress)) {
    return redirect(302, `/sorry?redirectTo=${encodeURIComponent(url.toString())}`);
  }

  return { form: searchParams, clientAddress };
};

export const actions: Actions = {
  default: async (event) => {
    const { request } = event;

    event.locals.form = await request.formData();

    return { success: true };
  },
} satisfies Actions;

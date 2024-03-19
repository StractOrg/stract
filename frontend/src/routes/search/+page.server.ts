import { extractSearchParams, type SearchParams } from '$lib/search';
import type { Actions, PageServerLoadEvent } from './$types';

export const load = async ({ locals, getClientAddress }: PageServerLoadEvent) => {
  const searchParams: SearchParams | undefined =
    (locals['form'] && extractSearchParams(locals['form'])) || undefined;

  return { form: searchParams, clientAddress: getClientAddress() };
};

export const actions: Actions = {
  default: async (event) => {
    const { request } = event;

    event.locals.form = await request.formData();

    return { success: true };
  },
} satisfies Actions;

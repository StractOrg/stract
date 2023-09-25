import { extractSearchParams, type SearchParams } from '$lib/search';
import type { Actions } from './$types';

export const load = async ({ locals }) => {
  // @ts-ignore
  const searchParams: SearchParams | undefined =
    (locals['form'] && extractSearchParams(locals['form'])) || undefined;
  return { form: searchParams };
};

export const actions: Actions = {
  default: async (event) => {
    const { request } = event;

    // @ts-ignore
    event.locals.form = await request.formData();

    return { success: true };
  },
} satisfies Actions;

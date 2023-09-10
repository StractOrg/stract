import { loadMarkdown } from '$lib/server/articles';
import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async () => {
  const { md } = await loadMarkdown('src/routes/webmasters/README.md');
  return { md, globals: { title: 'Webmasters' } };
};

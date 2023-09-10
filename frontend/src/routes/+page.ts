import type { PageLoad } from './$types';

export const load: PageLoad = () => ({ globals: { header: { hideLogo: true } } });

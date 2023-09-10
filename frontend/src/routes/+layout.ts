import { globals } from '$lib/globals';
import type { LayoutLoad } from './$types';

export const load: LayoutLoad = async () => ({ globals: await globals() });

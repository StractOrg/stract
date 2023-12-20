import { PUBLIC_API_BASE } from '$env/static/public';
import { setGlobalApiBase } from '$lib/api';
import { globals } from '$lib/globals';
import type { LayoutLoad } from './$types';

setGlobalApiBase(PUBLIC_API_BASE || 'http://0.0.0.0:3000');

export const load: LayoutLoad = async () => ({ globals: await globals() });

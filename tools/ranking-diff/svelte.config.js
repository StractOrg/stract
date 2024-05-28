import adapter from '@sveltejs/adapter-auto';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  // Consult https://kit.svelte.dev/docs/integrations#preprocessors
  // for more information about preprocessors
  preprocess: vitePreprocess(),

  kit: {
    csrf: {
      checkOrigin: false,
    },
    // adapter-auto only supports some environments, see https://kit.svelte.dev/docs/adapter-auto for a list.
    // If your environment is not supported, or you settled on a specific environment, switch out the adapter.
    // See https://kit.svelte.dev/docs/adapters for more information about adapters.
    adapter: adapter(),
    csp: {
      directives: {
        'default-src': ['*'],
        'script-src': ['self', 'wasm-unsafe-eval'],
        'connect-src': ['*'],
        'img-src': ['self', 'data:', 'stract.com', '0.0.0.0:3000', 'localhost:3000'],
        'style-src': ['self', 'unsafe-inline'],
      },
    },
  },
};

export default config;

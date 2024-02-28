import adapter from '@sveltejs/adapter-node';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

/** @type {import('@sveltejs/kit').Config} */
const config = {
  // Consult https://kit.svelte.dev/docs/integrations#preprocessors
  // for more information about preprocessors
  preprocess: vitePreprocess({
    style: {
      css: {
        postcss: join(__dirname, 'postcss.config.js'),
      },
    },
  }),

  kit: {
    csrf: {
      checkOrigin: false,
    },
    // adapter-auto only supports some environments, see https://kit.svelte.dev/docs/adapter-auto for a list.
    // If your environment is not supported or you settled on a specific environment, switch out the adapter.
    // See https://kit.svelte.dev/docs/adapters for more information about adapters.
    adapter: adapter(),
    csp: {
      directives: {
        'default-src': ['self'],
        'script-src': ['self'],
        // NOTE: Disabled in order to fetch optic sources client-side
        // 'connect-src': ["'self'", 'http://localhost:3000/'],
        'connect-src': ['*'],
        'img-src': ['self', 'data:', 'stract.com', '0.0.0.0:3000', 'localhost:3000'],
        'style-src': ['self', 'unsafe-inline'],
      },
    },
  },
};

export default config;

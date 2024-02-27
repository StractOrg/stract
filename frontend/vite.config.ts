import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';
import Icons from 'unplugin-icons/vite';
import wasm from 'vite-plugin-wasm';
import topLevelAwait from 'vite-plugin-top-level-await';

export default defineConfig({
  plugins: [
    wasm(),
    topLevelAwait(),
    sveltekit(),
    Icons({ compiler: 'svelte' }),
  ],
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
  server: {
    fs: {
      allow: ['../crates/client-wasm/pkg/']
    }
  }
});

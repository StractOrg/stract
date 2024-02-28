import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';
import Icons from 'unplugin-icons/vite';
import wasm from 'vite-plugin-wasm';
import topLevelAwait from 'vite-plugin-top-level-await';
import wasmPack from './src/wasm-pack-plugin';

export default defineConfig({
  plugins: [
    wasm(),
    wasmPack({
      crates: ['../crates/client-wasm/'],
    }),
    topLevelAwait(),
    sveltekit(),
    Icons({ compiler: 'svelte' }),
  ],
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
  server: {
    fs: {
      allow: ['../crates/client-wasm/pkg/'],
    },
  },
});

import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';
import Icons from 'unplugin-icons/vite';
import wasm from 'vite-plugin-wasm';
import wasmPack from './src/wasm-pack-plugin';

export default defineConfig({
  plugins: [
    sveltekit(),
    Icons({ compiler: 'svelte' }),
    wasm(),
    wasmPack({
      crates: ['../wasm/'],
    }),
  ],
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
});

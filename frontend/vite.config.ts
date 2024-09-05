import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';
import Icons from 'unplugin-icons/vite';
import wasm from 'vite-plugin-wasm';
import topLevelAwait from 'vite-plugin-top-level-await';
import wasmPack from './src/wasm-pack-plugin';
import copy from 'rollup-plugin-copy';
import path from 'path';
import { normalizePath } from 'vite';

export default defineConfig({
  plugins: [
    wasm(),
    wasmPack({
      crates: ['../crates/client-wasm/'],
    }),
    topLevelAwait(),
    sveltekit(),
    Icons({ compiler: 'svelte' }),
    copy({
      targets: [
        {
          src: normalizePath(path.resolve(__dirname, './src/lib/captcha/images')),
          dest: normalizePath(path.resolve(__dirname, './build/server/chunks')),
        },
        {
          src: normalizePath(path.resolve(__dirname, './src/lib/captcha/audio')),
          dest: normalizePath(path.resolve(__dirname, './build/server/chunks')),
        },
      ],
      hook: 'closeBundle',
    }),
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

import { sveltekit } from '@sveltejs/kit/vite';
import { paraglide } from '@inlang/paraglide-js-adapter-sveltekit/vite';
import { defineConfig } from 'vitest/config';
import Icons from 'unplugin-icons/vite';

export default defineConfig({
  plugins: [
    sveltekit(),
    paraglide({
      project: './project.inlang',
      outdir: './src/paraglide',
    }),
    Icons({ compiler: 'svelte' }),
  ],
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
});

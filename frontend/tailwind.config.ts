import { Config } from 'tailwindcss';
import * as defaultTheme from 'tailwindcss/defaultTheme';
import plugin from 'tailwindcss/plugin';

import typographyPlugin from '@tailwindcss/typography';
import formsPlugin from '@tailwindcss/forms';

const noscriptPlugin = plugin(({ addComponents }) => {
  // NOTE: We cannot choose where Tailwind puts the generated CSS, so we
  // manually add these classes to routes/+layout.svelte
  addComponents({
    '.noscript\\:hidden': {},
  });
});

export default {
  content: ['./src/**/*.{html,js,svelte,ts}'],
  theme: {
    extend: {
      colors: {
        snippet: '#4d5156',
        brand: {
          '50': 'rgb(237 249 255 / <alpha-value>)',
          '100': 'rgb(215 239 255 / <alpha-value>)',
          '200': 'rgb(185 228 255 / <alpha-value>)',
          '300': 'rgb(136 213 255 / <alpha-value>)',
          '400': 'rgb(80 189 255 / <alpha-value>)',
          '500': 'rgb(40 156 255 / <alpha-value>)',
          '600': 'rgb(11 123 255 / <alpha-value>)',
          '700': 'rgb(10 102 235 / <alpha-value>)',
          '800': 'rgb(15 82 190 / <alpha-value>)',
          '900': 'rgb(19 72 149 / <alpha-value>)',
          '950': 'rgb(17 45 90 / <alpha-value>)',
        },
        contrast: {
          '50': 'rgb(255 247 237 / <alpha-value>)',
          '100': 'rgb(255 236 212 / <alpha-value>)',
          '200': 'rgb(255 213 168 / <alpha-value>)',
          '300': 'rgb(255 183 112 / <alpha-value>)',
          '400': 'rgb(255 140 55 / <alpha-value>)',
          '500': 'rgb(255 105 11 / <alpha-value>)',
          '600': 'rgb(240 80 6 / <alpha-value>)',
          '700': 'rgb(199 58 7 / <alpha-value>)',
          '800': 'rgb(158 46 14 / <alpha-value>)',
          '900': 'rgb(127 41 15 / <alpha-value>)',
          '950': 'rgb(69 17 5 / <alpha-value>)',
        },
      },
      fontFamily: {
        sans: ['Helvetica', 'Arial', 'sans-serif', ...defaultTheme.fontFamily.sans],
      },
      animation: {
        blink: 'blink 1s steps(2) infinite',
      },
      keyframes: {
        blink: { '0%': { opacity: '0' } },
      },
    },
  },
  plugins: [typographyPlugin, formsPlugin, noscriptPlugin],
} satisfies Config;

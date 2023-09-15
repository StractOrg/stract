import { Config } from 'tailwindcss';
import * as defaultTheme from 'tailwindcss/defaultTheme';
import plugin from 'tailwindcss/plugin';

import typographyPlugin from '@tailwindcss/typography';
import formsPlugin from '@tailwindcss/forms';

import { themes } from './src/themes';

const noscriptPlugin = plugin(({ addComponents }) => {
  // NOTE: We cannot choose where Tailwind puts the generated CSS, so we
  // manually add these classes to routes/+layout.svelte
  addComponents({
    '.noscript\\:hidden': {},
  });
});

const color = (name: string) => ({
  [`${name}`]: `rgb(var(--${name}) / <alpha-value>)`,
});
const colorFocusContent = (name: string) => ({
  ...color(`${name}`),
  ...color(`${name}-focus`),
  ...color(`${name}-content`),
});

export default {
  content: ['./src/**/*.{html,js,svelte,ts}'],
  theme: {
    extend: {
      colors: {
        ...colorFocusContent('primary'),
        ...colorFocusContent('secondary'),
        ...colorFocusContent('accent'),
        ...colorFocusContent('neutral'),
        ...color('base-100'),
        ...color('base-200'),
        ...color('base-300'),
        ...color('base-400'),
        ...color('base-content'),
        ...colorFocusContent('info'),
        ...colorFocusContent('success'),
        ...colorFocusContent('warning'),
        ...colorFocusContent('error'),
        ...color('link'),
        ...color('link-visited'),
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
  plugins: [typographyPlugin, formsPlugin, noscriptPlugin, ...themes],
} satisfies Config;

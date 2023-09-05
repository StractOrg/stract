// NOTE: This file exists only to get the Tailwind VSCode extension to kick in.
//       See https://github.com/denoland/fresh/issues/1519 for details.

/** @type {import('tailwindcss').Config} */
export default {
  content: ["./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}"],
  theme: {
    extend: {
      colors: {
        snippet: "#4d5156",
        brand: {
          "50": "rgb(var(--brand-50) / <alpha-value>)",
          "100": "rgb(var(--brand-100) / <alpha-value>)",
          "200": "rgb(var(--brand-200) / <alpha-value>)",
          "300": "rgb(var(--brand-300) / <alpha-value>)",
          "400": "rgb(var(--brand-400) / <alpha-value>)",
          "500": "rgb(var(--brand-500) / <alpha-value>)",
          "600": "rgb(var(--brand-600) / <alpha-value>)",
          "700": "rgb(var(--brand-700) / <alpha-value>)",
          "800": "rgb(var(--brand-800) / <alpha-value>)",
          "900": "rgb(var(--brand-900) / <alpha-value>)",
          "950": "rgb(var(--brand-950) / <alpha-value>)",
        },
        contrast: {
          "50": "rgb(var(--contrast-50) / <alpha-value>)",
          "100": "rgb(var(--contrast-100) / <alpha-value>)",
          "200": "rgb(var(--contrast-200) / <alpha-value>)",
          "300": "rgb(var(--contrast-300) / <alpha-value>)",
          "400": "rgb(var(--contrast-400) / <alpha-value>)",
          "500": "rgb(var(--contrast-500) / <alpha-value>)",
          "600": "rgb(var(--contrast-600) / <alpha-value>)",
          "700": "rgb(var(--contrast-700) / <alpha-value>)",
          "800": "rgb(var(--contrast-800) / <alpha-value>)",
          "900": "rgb(var(--contrast-900) / <alpha-value>)",
          "950": "rgb(var(--contrast-950) / <alpha-value>)",
        },
      },
    },
  },
};

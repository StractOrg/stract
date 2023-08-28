// NOTE: This file exists only to get the Tailwind VSCode extension to kick in.
//       See https://github.com/denoland/fresh/issues/1519 for details.

/** @type {import('tailwindcss').Config} */
export default {
  content: ["./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}"],
  theme: {
    extend: {
      colors: {
        brand: "rgb(11 123 255 / <alpha-value>)",
        brand_contrast: "rgb(255 105 11 / <alpha-value>)",
        snippet: "#4d5156",
      },
    },
  },
};

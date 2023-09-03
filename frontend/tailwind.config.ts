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
        // brand: {
        //   "50": "#edf9ff",
        //   "100": "#d7efff",
        //   "200": "#b9e4ff",
        //   "300": "#88d5ff",
        //   "400": "#50bdff",
        //   "500": "#289cff",
        //   "600": "#0b7bff",
        //   "700": "#0a66eb",
        //   "800": "#0f52be",
        //   "900": "#134895",
        //   "950": "#112d5a",
        // },
      },
    },
  },
};

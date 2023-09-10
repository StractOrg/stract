import type { Config } from "https://esm.sh/tailwindcss@3.3.3";

// NOTE: This file exists only to get the Tailwind VSCode extension to kick in.
//       See https://github.com/denoland/fresh/issues/1519 for details.

export default {
  content: ["./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}"],
  theme: {
    extend: {
      colors: {
        snippet: "#4d5156",
        brand: {
          "50": "rgb(237 249 255 / <alpha-value>)",
          "100": "rgb(215 239 255 / <alpha-value>)",
          "200": "rgb(185 228 255 / <alpha-value>)",
          "300": "rgb(136 213 255 / <alpha-value>)",
          "400": "rgb(80 189 255 / <alpha-value>)",
          "500": "rgb(40 156 255 / <alpha-value>)",
          "600": "rgb(11 123 255 / <alpha-value>)",
          "700": "rgb(10 102 235 / <alpha-value>)",
          "800": "rgb(15 82 190 / <alpha-value>)",
          "900": "rgb(19 72 149 / <alpha-value>)",
          "950": "rgb(17 45 90 / <alpha-value>)",
        },
        contrast: {
          "50": "rgb(255 247 237 / <alpha-value>)",
          "100": "rgb(255 236 212 / <alpha-value>)",
          "200": "rgb(255 213 168 / <alpha-value>)",
          "300": "rgb(255 183 112 / <alpha-value>)",
          "400": "rgb(255 140 55 / <alpha-value>)",
          "500": "rgb(255 105 11 / <alpha-value>)",
          "600": "rgb(240 80 6 / <alpha-value>)",
          "700": "rgb(199 58 7 / <alpha-value>)",
          "800": "rgb(158 46 14 / <alpha-value>)",
          "900": "rgb(127 41 15 / <alpha-value>)",
          "950": "rgb(69 17 5 / <alpha-value>)",
        },
      },
    },
  },
} satisfies Config;

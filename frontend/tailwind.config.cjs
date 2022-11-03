const defaultTheme = require("tailwindcss/defaultTheme");

/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}"],
  theme: {
    extend: {
      colors: {
        brand: "rgb(11 123 255 / <alpha-value>)",
        snippet: "#4d5156",
      },
      fontFamily: {
        sans: [
          // "MontserratVariable",
          // "Montserrat",
          "Helvetica",
          "Arial",
          "sans-serif",
          ...defaultTheme.fontFamily.sans,
        ],
      },
    },
  },
  plugins: [require("@tailwindcss/typography")],
};

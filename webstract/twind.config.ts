import { defineConfig } from "https://esm.sh/@twind/core@1.1.3";
import presetTailwind from "https://esm.sh/@twind/preset-tailwind@1.1.4";
import presetAutoprefix from "https://esm.sh/@twind/preset-autoprefix@1.0.7";
import presetLineClamp from "https://esm.sh/@twind/preset-line-clamp@1.0.7";
import presetTypography from "https://esm.sh/@twind/preset-typography@1.0.7";
import presetForms from "https://esm.sh/@twind/preset-tailwind-forms@1.1.2";

export default {
  ...defineConfig({
    darkMode: "media",
    theme: {
      extend: {
        colors: {
          brand: "rgb(11 123 255 / <alpha-value>)",
          brand_contrast: "rgb(255 105 11 / <alpha-value>)",
          snippet: "#4d5156",
        },
        fontFamily: {
          sans: [
            "Helvetica",
            "Arial",
            "sans-serif",
            // ...defaultTheme.fontFamily.sans,
          ],
        },
      },
    },
    presets: [
      presetTailwind(),
      presetAutoprefix(),
      presetLineClamp(),
      presetTypography(),
      presetForms(),
    ],
  }),
  selfURL: import.meta.url,
};

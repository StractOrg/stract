import { defineConfig } from "./unocss-plugin.ts";
import presetUno from "https://esm.sh/@unocss/preset-uno@0.55.7";
import presetTypography from "https://esm.sh/@unocss/preset-typography@0.55.7";
import { presetForms } from "https://esm.sh/@julr/unocss-preset-forms@0.0.5?@unocss/preset-mini:@unocss/preset-mini@0.55.7,unocss:unocss@0.55.7";

import tailwindConfig from "./tailwind.config.ts";

export default defineConfig({
  theme: {
    ...tailwindConfig.theme.extend,
  },
  rules: [
    [/^animate-typing$/, () => {
      return `
        .animate-typing {
          animation: mercuryTyping 1.8s infinite ease-in-out;
        }

        @keyframes mercuryTyping {
          0% {
            transform: translateY(0px);
            opacity: 100%;
          }
          28% {
            transform: translateY(-7px);
            opacity: 40%;
          }
          44% {
            transform: translateY(0px);
            opacity: 20%;
          }
        }
      `;
    }],
    [/^animate-blink$/, () => {
      return `
        .animate-blink {
          animation: blink 1s steps(2) infinite;
        }

        @keyframes blink {
          0% {
            opacity: 0;
          }
        }
      `;
    }],
  ],
  presets: [
    presetUno({ dark: "media" }),
    presetForms(),
    presetTypography(),
  ],
  selfURL: import.meta.url,
});

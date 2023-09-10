import { defineConfig } from "$fresh/server.ts";
import unocssConfig from "./uno.config.ts";
import unocssPlugin from "./unocss-plugin.ts";

export default defineConfig({
  plugins: [unocssPlugin(unocssConfig)],
});

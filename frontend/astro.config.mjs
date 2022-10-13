import path from "path";
import { defineConfig } from "astro/config";
import tailwind from "@astrojs/tailwind";
import mdx from "@astrojs/mdx";

// https://astro.build/config
export default defineConfig({
  vite: {
    resolve: {
      alias: {
        $: path.resolve("./src"),
      },
    },
  },
  integrations: [tailwind(), mdx()],
});

import { defineRoute } from "$fresh/server.ts";
import { Article } from "../components/Article.tsx";
import { Markdown } from "../components/Markdown.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default defineRoute(async () => {
  const file = import.meta.resolve("./webmasters.md").slice("file://".length);
  const content = await Deno.readTextFile(file);

  return (
    <Article title="Stract Crawler">
      <Markdown src={content} />
    </Article>
  );
});

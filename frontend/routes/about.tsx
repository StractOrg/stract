import { Article } from "../components/Article.tsx";
import { Markdown } from "../components/Markdown.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default async function About() {
  const file = import.meta.resolve("./about.md").slice("file://".length);
  const content = await Deno.readTextFile(file);

  return (
    <Article title="About us">
      <Markdown src={content} />
    </Article>
  );
}

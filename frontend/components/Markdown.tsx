import { render } from "https://deno.land/x/gfm@0.2.5/mod.ts";

export const Markdown = ({ src }: { src: string }) => {
  return (
    <div
      dangerouslySetInnerHTML={{ __html: render(src) }}
    />
  );
};

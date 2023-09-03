import { render } from "https://deno.land/x/gfm@0.2.5/mod.ts";
import { injectGlobal } from "https://esm.sh/@twind/core@1.1.3";

export const Markdown = ({ src }: { src: string }) => {
  return (
    <div
      dangerouslySetInnerHTML={{ __html: render(src) }}
    />
  );
};

injectGlobal`
.prose .anchor {
    @apply hidden;
}
`;

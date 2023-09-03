import { tx } from "https://esm.sh/@twind/core@1.1.3";
import { ComponentChildren } from "preact";

export const Site = (
  { href, right, children }: {
    href: string;
    right?: ComponentChildren;
    children?: ComponentChildren;
  },
) => (
  <span class="bg-brand/5 transition rounded-lg flex">
    <a
      href={href}
      class={tx(
        "le-site text-brand/90 hover:text-brand hover:no-underline py-2",
        right ? "pl-3 pr-2" : "px-3",
      )}
    >
      {children ?? href}
    </a>
    {right && (
      <>
        <span class="w-px bg-brand/10 my-2" />
        {right}
      </>
    )}
  </span>
);

export const SiteWithDelete = (
  { href, children, onDelete }: {
    href: string;
    children?: ComponentChildren;
    onDelete: () => void;
  },
) => (
  <Site
    href={href}
    right={
      <button
        class="remove-site cursor-pointer bg-transparent px-2 text-brand/40 text-sm hover:text-brand transition"
        onClick={() => onDelete()}
      >
        ×
      </button>
    }
  >
    {children}
  </Site>
);

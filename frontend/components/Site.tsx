import { twMerge } from "tailwind-merge";
import { ComponentChildren } from "preact";

export const Site = (
  { href, right, children }: {
    href: string;
    right?: ComponentChildren;
    children?: ComponentChildren;
  },
) => (
  <span class="group bg-brand-500 transition rounded-lg flex dark:bg-brand-800 overflow-hidden">
    <a
      href={href}
      class={twMerge(`
        transition py-2
        text-white
        bg-brand-500 hover:bg-brand-600 active:bg-brand-700
        dark:bg-brand-800 dark:hover:bg-brand-700 dark:active:bg-brand-600
        ${right ? "pl-3 pr-2" : "px-3"}
      `)}
    >
      {children ?? href}
    </a>
    {right && (
      <>
        <span class="w-px bg-brand-100 my-2 dark:bg-brand-950 group-hover:bg-transparent transition" />
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
        class={twMerge(`
          remove-site cursor-pointer px-2 text-sm transition
        text-white
        bg-brand-500 hover:bg-brand-600 active:bg-brand-700
          dark:bg-brand-800 dark:hover:bg-brand-700 dark:active:bg-brand-600
        `)}
        onClick={() => onDelete()}
      >
        ×
      </button>
    }
  >
    {children}
  </Site>
);

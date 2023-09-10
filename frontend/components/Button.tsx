import { twMerge } from "tailwind-merge";
import { JSX } from "preact";

export function Button(
  { pale, base = pale ? "gray" : "brand", class: class_, className, ...props }:
    & { base?: string; pale?: boolean }
    & JSX.HTMLAttributes<HTMLButtonElement>,
) {
  return (
    <button
      class={twMerge(`
        rounded-full px-2 py-2 md:px-5 text-sm transition-colors
      ${
        pale
          ? `
          text-${base}-900 dark:text-${base}-50
          bg-white hover:bg-${base}-50 active:bg-${base}-100
          dark:bg-transparent dark:hover:bg-${base}-800 dark:active:bg-${base}-700
          border
          border-${base}-100 hover:border-${base}-200 active:border-${base}-300
          dark:border-${base}-600 dark:hover:border-${base}-500 dark:active:border-${base}-400
          `
          : `
          text-white dark:text-white
          border border-transparent
          bg-${base}-500 hover:bg-${base}-600 active:bg-${base}-700
          dark:bg-${base}-800 dark:hover:bg-${base}-700 dark:active:bg-${base}-600
          `
      }
        ${class_?.toString()}
        ${className?.toString()}
      `)}
      {...props}
    />
  );
}

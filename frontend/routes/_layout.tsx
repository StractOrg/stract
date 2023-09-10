import { LayoutProps } from "$fresh/server.ts";

export default function Layout({ Component }: LayoutProps) {
  return (
    <div class="font-light h-full antialiased dark:bg-stone-900 dark:text-white font-[Helvetica,Arial,sans-serif]">
      <Component />
    </div>
  );
}

import { ComponentChildren } from "preact";
import { Header } from "./Header.tsx";
import { Footer } from "./Footer.tsx";
import { Head } from "$fresh/runtime.ts";
import { twMerge } from "tailwind-merge";

export const Article = (
  { title, children }: { title: string; children: ComponentChildren },
) => {
  const with_alice = false;

  return (
    <>
      <Head>
        <title>{title}</title>
      </Head>
      <div class="flex h-full w-full flex-col">
        <Header withAlice={with_alice} />

        <div class="flex h-fit w-full flex-col items-center pt-10 px-5">
          <div
            class={twMerge(
              "prose text-sm dark:prose-invert leading-6 [&_.anchor]:hidden",
              "[&_h1]:text-center [&_h1]:font-medium",
              "[&_h2]:text-center [&_h2]:font-medium",
              "[&_h3]:text-center [&_h3]:font-medium",
              "[&_h4]:text-center [&_h4]:font-medium",
              "[&_h4]:float-left [&_h4]:my-0 [&_h4]:mr-3 [&_h4]:leading-7",
              "[&_li]:my-2",
            )}
          >
            {children}
          </div>
        </div>
        <div class="flex flex-grow"></div>
        <Footer />
      </div>
    </>
  );
};

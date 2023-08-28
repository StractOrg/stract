import { ComponentChildren } from "preact";
import { Header } from "./Header.tsx";
import { Footer } from "./Footer.tsx";
import { Head } from "$fresh/runtime.ts";

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
          <div class="prose prose-sm prose-headings:text-center prose-headings:font-medium prose-h4:float-left prose-h4:my-0 prose-h4:mr-3 prose-h4:leading-7">
            {children}
          </div>
        </div>
        <div class="flex flex-grow"></div>
        <Footer />
      </div>
    </>
  );
};

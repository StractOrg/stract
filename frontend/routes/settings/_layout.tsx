import { LayoutProps } from "$fresh/server.ts";
import { twMerge } from "tailwind-merge";
import { Footer } from "../../components/Footer.tsx";
import { Header } from "../../components/Header.tsx";

export default function Layout({ Component, url }: LayoutProps) {
  const with_alice = false;

  const queryUrlPart = url.searchParams.toString();

  return (
    <>
      <div class="flex h-full w-full flex-col">
        <Header withAlice={with_alice} queryUrlPart={queryUrlPart} />

        <div class="flex h-fit w-full justify-center pt-10">
          <SettingsMenu queryUrlPart={queryUrlPart} />
          <div class="flex w-full max-w-2xl flex-col">
            <Component />
          </div>
        </div>
        <div class="flex flex-grow"></div>
        <Footer />
      </div>
    </>
  );
}

const SettingsMenu = ({ queryUrlPart: query }: {
  queryUrlPart?: string;
}) => {
  const links = [
    { url: "/settings", title: "Preferences" },
    { url: "/settings/optics", title: "Manage Optics" },
    { url: "/settings/sites", title: "Site Rankings" },
    { url: "/settings/privacy", title: "Privacy" },
  ];

  return (
    <div class="relative right-0 md:right-5 lg:right-20 flex flex-col space-y-3">
      {links.map((l) => (
        <a
          class={twMerge(`
            transition hover:no-underline rounded-full px-2 py-1 text-center
            hover:bg-brand-50 hover:text-brand-600
            dark:hover:bg-brand-900 dark:hover:text-stone-50
          `)}
          href={`${l.url}${query ? "?" + query : ""}`}
        >
          {l.title}
        </a>
      ))}
    </div>
  );
};

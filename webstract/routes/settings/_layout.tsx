import { LayoutProps } from "$fresh/server.ts";
import { Footer } from "../../components/Footer.tsx";
import { Header } from "../../components/Header.tsx";

export default function Layout({ Component, state }: LayoutProps) {
  const with_alice = false;

  return (
    <>
      <div class="flex h-full w-full flex-col">
        <Header withAlice={with_alice} />

        <div class="flex h-fit w-full justify-center pt-10">
          <SettingsMenu />
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

const SettingsMenu = () => {
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
          class="transition hover:bg-brand/5 hover:text-brand hover:no-underline rounded-full px-2 py-1 text-center"
          href={l.url}
        >
          {l.title}
        </a>
      ))}
    </div>
  );
};

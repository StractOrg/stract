import { Navbar } from "./Navbar.tsx";

export type HeaderProps = {
  active?: Section;
  withAlice?: boolean;
  showDivider?: boolean;
  showLogo?: boolean;
  queryUrlPart?: string;
};
export const Header = (
  {
    active,
    queryUrlPart,
    withAlice = false,
    showDivider = false,
    showLogo = true,
  }: HeaderProps,
) => (
  <>
    <div class="flex shrink-0 h-10 justify-between items-center px-4 sm:gap-4 md:gap-5 lg:gap-10">
      <div>
        <div class="flex md:space-x-2 lg:space-x-4 text-sm relative z-0 bottom-0">
          <IndexBar
            active={active}
            queryUrlPart={queryUrlPart}
            withAlice={withAlice}
          />
        </div>
      </div>
      {showLogo && (
        <div class="w-20 absolute left-1/2 translate-x-[-50%]">
          <a href="/">
            <img src="/images/biglogo-beta.svg" />
          </a>
        </div>
      )}
      <div>
        <Navbar queryUrlPart={queryUrlPart} />
      </div>
    </div>

    {showDivider && (
      <div class="w-full h-[1px] bg-gradient-to-r from-cyan-400 via-brand to-cyan-400">
      </div>
    )}
  </>
);

type Section = "Search" | "Explore" | "Chat";

export const IndexBar = (
  { active, queryUrlPart: query, withAlice }: {
    active?: Section;
    queryUrlPart?: string;
    withAlice: boolean;
  },
) => {
  const links = [
    { url: "/search", title: "Search" },
    { url: "/explore", title: "Explore" },
    { url: "/chat", title: "Chat" },
  ] satisfies { url: string; title: Section }[];

  let availableLinks: { url: string; title: Section }[] = [];
  if (withAlice) {
    availableLinks = links;
  } else {
    availableLinks = links.filter((l) => l.title != "Chat");
  }

  return (
    <>
      {availableLinks.map((l) => (
        <div class="inline relative z-0">
          <a
            class="link px-2 py-1 rounded-full text-sm"
            href={`${l.url}${query ? "?" + query : ""}`}
          >
            {l.title}
          </a>
          {active === l.title && (
            <div class="w-full h-[1px] absolute left-0 right-0 bottom-[-11px] bg-brand_contrast">
            </div>
          )}
        </div>
      ))}
    </>
  );
};

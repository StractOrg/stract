import { tx } from "https://esm.sh/@twind/core@1.1.3";
import { HiBars2 } from "../icons/HiBars2.tsx";
import { SiDiscord } from "../icons/SiDiscord.tsx";
import { SiGitHub } from "../icons/SiGitHub.tsx";
import { JSX } from "preact/jsx-runtime";

const links = [
  { url: "/settings", title: "Settings" },
  { url: "/about", title: "About" },
];
const icons = [
  {
    url: "https://discord.gg/BmzKHffWJM",
    title: "Discord",
    Icon: SiDiscord,
  },
  {
    url: "https://github.com/StractOrg/stract",
    title: "GitHub",
    Icon: SiGitHub,
  },
];

export const Navbar = ({ queryUrlPart: query }: {
  queryUrlPart?: string;
}) => (
  <>
    <nav class="hidden items-center space-x-1 sm:flex md:space-x-2 lg:space-x-4">
      {links.map((l) => (
        <Link
          class="rounded-full text-sm px-2 py-1"
          href={`${l.url}${query ? "?" + query : ""}`}
        >
          {l.title}
        </Link>
      ))}
      {icons.map((i) => (
        <Icon href={i.url}>
          <i.Icon class="w-4" title={i.title} />
        </Icon>
      ))}
    </nav>

    <nav class="group relative flex items-center text-lg sm:hidden">
      <button class="mx-1 aspect-square rounded-full bg-transparent px-3 text-gray-400 transition group-hover:text-brand/30">
        <HiBars2 class="w-6" />
      </button>
      <div class="pointer-events-none absolute bottom-0 right-0 z-50 translate-y-full flex-col pt-1 opacity-0 transition group-hover:pointer-events-auto group-hover:flex group-hover:opacity-100">
        <div class="rounded-xl border bg-white p-2 shadow-xl">
          <div class="flex flex-col space-y-1 pb-2">
            {links.map((l) => (
              <Link class="rounded py-1 pl-2 pr-10" href={l.url}>
                {l.title}
              </Link>
            ))}
          </div>
          <div class="flex justify-around border-t pt-2">
            {icons.map((i) => (
              <Icon href={i.url}>
                <i.Icon class="w-7" title={i.title} />
              </Icon>
            ))}
          </div>
        </div>
      </div>
    </nav>
  </>
);

const Icon = (
  { "class": c, ...props }: JSX.HTMLAttributes<HTMLAnchorElement>,
) => (
  <a
    class={tx(
      "flex justify-center rounded-full p-2 text-gray-500 transition hover:bg-brand/10 hover:text-brand",
      c?.toString(),
    )}
    {...props}
  />
);

const Link = (
  { "class": c, ...props }: JSX.HTMLAttributes<HTMLAnchorElement>,
) => (
  <a
    class={tx(
      "transition hover:bg-brand/5 hover:text-brand hover:no-underline",
      c?.toString(),
    )}
    {...props}
  />
);

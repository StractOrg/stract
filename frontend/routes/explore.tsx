import { defineRoute } from "$fresh/server.ts";
import { Footer } from "../components/Footer.tsx";
import { Header } from "../components/Header.tsx";
import { ExploreSites } from "../islands/ExploreSites.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default defineRoute((req) => {
  const with_alice = false;

  return (
    <div class="relative grid h-full grid-rows-[auto_1fr_auto]">
      <div class="row-start-1">
        <Header
          withAlice={with_alice}
          showDivider={true}
          active="Explore"
          queryUrlPart={new URL(req.url).searchParams.toString()}
        />
      </div>
      <div class="row-start-2 flex mt-10 px-5 justify-center">
        <ExploreSites />
      </div>
      <div class="row-start-3">
        <Footer />
      </div>
    </div>
  );
});

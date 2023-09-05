import { Searchbar } from "../islands/Searchbar.tsx";
import { Header } from "../components/Header.tsx";
import { Footer } from "../components/Footer.tsx";
import { OpticSelector } from "../islands/OpticsSelector.tsx";
import { DEFAULT_OPTICS } from "../search/optics.ts";
import { DEFAULT_ROUTE_CONFIG } from "../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default function Home() {
  const with_alice = false;

  return (
    <div class="grid h-full grid-rows-[25vh_1fr_auto]">
      <div>
        <Header withAlice={with_alice} showLogo={false} />
      </div>
      <div class="grid w-full items-start place-items-center">
        <div class="flex w-full flex-col items-center justify-center px-2 md:max-w-2xl md:px-0">
          <div class="flex w-64 mb-6">
            <img src="/images/biglogo-with-text.svg" class="h-full w-full" />
          </div>
          <Searchbar autofocus />
          <div class="mt-3 text-gray-600 flex space-x-2 dark:text-gray-500">
            <div>
              Customise your search with an{" "}
              <a class="underline font-medium" href="/settings">optic</a>:
            </div>
            <OpticSelector
              defaultOptics={DEFAULT_OPTICS}
              searchOnChange={false}
            />
          </div>
        </div>
      </div>

      <div class="row-start-3">
        <Footer />
      </div>
    </div>
  );
}

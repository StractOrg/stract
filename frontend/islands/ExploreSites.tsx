import { HiChevronDown } from "../icons/HiChevronDown.tsx";
import { useSignal, useSignalEffect } from "@preact/signals";
import { IS_BROWSER } from "$fresh/runtime.ts";
import * as search from "../search/index.ts";
import { SiteWithDelete } from "../components/Site.tsx";
import { match } from "ts-pattern";
import { Button } from "../components/Button.tsx";
import { HiPlusCircleOutline } from "../icons/HiPlusCircleOutline.tsx";
import { HiXCircleOutline } from "../icons/HiXCircleOutline.tsx";
import { twMerge } from "tailwind-merge";

const LIMIT_OPTIONS = [
  10,
  25,
  50,
  125,
  250,
  500,
  1000,
];

export const ExploreSites = () => {
  const limit = useSignal(10);
  const errorMessage = useSignal(false);
  const inputWebsite = useSignal("");
  const sites = useSignal<string[]>([]);
  const loading = useSignal(false);

  const similarSites = useSignal<search.ScoredSite[]>([]);

  useSignalEffect(() => {
    if (sites.value.length == 0 || limit.value <= 0) return;

    loading.value = true;
    const { data, cancel } = search.api.similarSites(
      {
        sites: sites.value,
        topN: limit.value,
      },
    );

    data.then((res) => {
      loading.value = false;
      similarSites.value = res;
    });

    return () => cancel();
  });

  return (
    <>
      <noscript>
        <div class="flex flex-col grow max-w-3xl">
          <div class="text-red-600">
            Unfortunately, this site requires javascript to function.
          </div>
        </div>
      </noscript>
      <div class="flex flex-col grow max-w-3xl noscirpt:hidden">
        <div class="flex flex-col mb-4 items-center">
          <div class="flex flex-col space-y-1 items-center mb-5">
            <h1 class="text-2xl font-bold">Explore the web</h1>
            <p class="text-center">
              Find sites similar to your favorites and discover hidden gems you
              never knew existed.
            </p>
          </div>
          <form
            class={twMerge(`
            flex rounded-full w-full max-w-lg p-[2px] pl-3 mb-2
            transition focus-within:shadow
            border border-gray-300 dark:border-stone-700 focus-within:dark:border-stone-600
            bg-white dark:bg-stone-800
          `)}
            id="site-input-container"
            onSubmit={(e) => {
              e.preventDefault();
              if (inputWebsite.value == "") return;

              errorMessage.value = false;

              search.api.knowsSite({
                site: inputWebsite.value,
              }).data.then((res) => {
                match(res).with({ type: "known" }, () => {
                  sites.value = [...sites.value, inputWebsite.value];
                  inputWebsite.value = "";
                }).with({ type: "unknown" }, () => {
                  errorMessage.value = true;
                }).exhaustive();
              });
            }}
          >
            <input
              class="border-none outline-none bg-transparent focus:ring-0 grow placeholder:opacity-50"
              type="text"
              id="site-input"
              name="site"
              autofocus
              placeholder="www.example.com"
              value={inputWebsite}
              onInput={(e) =>
                inputWebsite.value = (e.target as HTMLInputElement).value}
            />
            <Button id="add-site-btn">
              Add
            </Button>
          </form>
          {errorMessage.value && (
            <label
              class="text-red-600 mb-4"
              for="site-input"
              id="site-input-error"
            >
              Unfortunately, we don't know about that site yet.
            </label>
          )}
          <div
            class="flex flex-wrap gap-x-5 gap-y-3 justify-center"
            id="sites-list"
          >
            {sites.value.map((site) => (
              <SiteWithDelete
                key={site}
                href={site}
                onDelete={() => {
                  sites.value = sites.value.filter((s) => s != site);
                }}
              >
                {site}
              </SiteWithDelete>
            ))}
          </div>
        </div>

        {sites.value.length > 0 &&
          (
            <div id="result-container">
              <div class="flex items-center justify-between mb-5">
                <div class="flex items-center space-x-5">
                  <h2 class="text-2xl font-bold">Similar sites</h2>
                  <div class="flex space-x-1">
                    <select
                      id="limit"
                      class="styled-selector border-none cursor-pointer rounded dark:bg-transparent"
                      value={limit.value}
                      onChange={(e) =>
                        limit.value = parseInt(
                          (e.target as HTMLSelectElement).value,
                        )}
                    >
                      {LIMIT_OPTIONS.map((l) => <option value={l}>{l}</option>)}
                    </select>
                  </div>
                </div>
                <Button
                  onClick={async () => {
                    const { data } = search.api.exploreExportOptic({
                      chosenSites: sites.value,
                      similarSites: similarSites.value.map((s) => s.site),
                    });
                    const { default: fileSaver } = await import("file-saver");
                    fileSaver.saveAs(new Blob([await data]), "exported.optic");
                  }}
                >
                  Export as optic
                </Button>
              </div>
              <div
                id="result"
                class="grid grid-cols-[auto_auto_minmax(auto,66%)] gap-x-3 gap-y-2 items-center"
              >
                {similarSites.value.map((site) => (
                  <>
                    <button
                      class={twMerge(`
                      group transition
                      text-green-500 disabled:text-gray-400 enabled:hover:text-green-400 active:text-green-300
                    `)}
                      disabled={sites.value.includes(site.site)}
                      onClick={() => sites.value = [...sites.value, site.site]}
                    >
                      <HiXCircleOutline class="w-6 hidden group-disabled:block" />
                      <HiPlusCircleOutline class="w-6 block group-disabled:hidden" />
                    </button>
                    <div class="tabular-nums">{site.score.toFixed(2)}</div>
                    <div>
                      <a
                        target="_blank"
                        class="underline"
                        href={`https://${site.site}`}
                      >
                        {site.site}
                      </a>
                      <p class="text-sm">{site.description}</p>
                    </div>
                  </>
                ))}
              </div>
              <div class="w-full flex justify-center">
                <div
                  class="w-6 h-6 cursor-pointer rounded-full text-contrast-500 dark:text-contrast-700"
                  id="more-btn"
                  onClick={() => {
                    if (
                      limit.value == LIMIT_OPTIONS[LIMIT_OPTIONS.length - 1]
                    ) {
                      return;
                    }
                    limit.value =
                      LIMIT_OPTIONS[LIMIT_OPTIONS.indexOf(limit.value) + 1];
                  }}
                >
                  <HiChevronDown />
                </div>
              </div>
            </div>
          )}
      </div>
    </>
  );
};

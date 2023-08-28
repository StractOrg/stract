import { HiChevronDown } from "../icons/HiChevronDown.tsx";
import { useSignal, useSignalEffect } from "@preact/signals";
import LZString from "npm:lz-string";
import { IS_BROWSER } from "$fresh/runtime.ts";
import * as search from "../search/index.ts";
import { Site, SiteWithDelete } from "../components/Site.tsx";
import { match } from "ts-pattern";

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
  if (!IS_BROWSER) {
    return (
      <div class="flex flex-col grow max-w-3xl">
        <noscript>
          <div class="text-red-600">
            Unfortunately, this site requires javascript to function.
          </div>
        </noscript>
      </div>
    );
  }

  const limit = useSignal(10);
  const errorMessage = useSignal(false);
  const inputWebsite = useSignal("");
  const sites = useSignal<string[]>(["facebook.com"]);
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

  const data: { chosen_sites: string[]; similar_sites: string[] } = {
    chosen_sites: sites.value,
    similar_sites: similarSites.value.map((s) => s.site),
  };
  const compressed = LZString.compressToBase64(JSON.stringify(data));
  const exportUrl = `${window.location}/export?data=${compressed}`;

  return (
    <div class="flex flex-col grow max-w-3xl">
      <div class="flex flex-col mb-4 items-center">
        <div class="flex flex-col space-y-1 items-center mb-5">
          <h1 class="text-2xl font-bold">Explore the web</h1>
          <p class="text-center">
            Find sites similar to your favorites and discover hidden gems you
            never knew existed.
          </p>
        </div>
        <form
          class="flex border rounded-full w-full max-w-lg p-[2px] pl-3 mb-2"
          id="site-input-container"
          onSubmit={(e) => {
            e.preventDefault();
            if (inputWebsite.value == "") return;

            errorMessage.value = false;

            search.api.knowsSite({
              site: inputWebsite.value,
            }).data.then((res) => {
              match(res).with({ "@type": "known" }, () => {
                sites.value = [...sites.value, inputWebsite.value];
                inputWebsite.value = "";
              }).with({ "@type": "unknown" }, () => {
                errorMessage.value = true;
              }).exhaustive();
            });
          }}
        >
          <input
            class="outline-none focus:ring-0 grow"
            type="text"
            id="site-input"
            name="site"
            autofocus
            placeholder="www.example.com"
            value={inputWebsite}
            onInput={(e) =>
              inputWebsite.value = (e.target as HTMLInputElement).value}
          />
          <button
            id="add-site-btn"
            class="bg-brand text-sm text-white opacity-75 hover:opacity-100 transition-colors duration-50 rounded-full py-2 px-5"
          >
            Add
          </button>
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
        <div class="flex flex-wrap gap-x-5 gap-y-3" id="sites-list">
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
                    class="styled-selector"
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
              <a
                href={exportUrl}
                download="exported.optic"
                class="bg-brand text-white opacity-75 hover:opacity-100 transition-colors duration-50 rounded-full py-2 px-5"
                id="export-optic"
              >
                Export as optic
              </a>
            </div>
            <div id="result" class="grid grid-cols-[1fr_1fr_6fr] gap-y-2">
              {similarSites.value.map((site) => (
                <>
                  {sites.value.includes(site.site)
                    ? (
                      <div class="w-4">
                        <img src="/images/disabled-add.svg" />
                      </div>
                    )
                    : (
                      <div
                        class="w-4 cursor-pointer"
                        onClick={() => {
                          sites.value = [...sites.value, site.site];
                        }}
                      >
                        <img src="/images/add.svg" />
                      </div>
                    )}
                  <div>{site.score.toFixed(2)}</div>
                  <div>
                    <a
                      target="_blank"
                      class="underline"
                      href={site.site}
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
                class="w-6 h-6 cursor-pointer rounded-full text-brand_contrast"
                id="more-btn"
                onClick={() => {
                  if (limit.value == LIMIT_OPTIONS[LIMIT_OPTIONS.length - 1]) {
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
  );
};

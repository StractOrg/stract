import { defineRoute } from "$fresh/server.ts";
import { Searchbar } from "../islands/Searchbar.tsx";
import { injectGlobal, tx } from "https://esm.sh/@twind/core@1.1.3";
import * as search from "../search/index.ts";
import { match, P } from "ts-pattern";
import { Header } from "../components/Header.tsx";
import { OpticSelector } from "../islands/OpticsSelector.tsx";
import SearchResultAdjust, {
  SearchResultAdjustModal,
  SelectedAdjust,
} from "../islands/SearchResultAdjust.tsx";
import { Head } from "$fresh/runtime.ts";
import { Sidebar } from "../components/Sidebar.tsx";
import { Footer } from "../components/Footer.tsx";
import { Signal, signal } from "@preact/signals";
import { Snippet } from "../islands/Snippet.tsx";
import { DEFAULT_OPTICS } from "../search/optics.ts";
import { HiChevronLeft } from "../icons/HiChevronLeft.tsx";
import { HiChevronRight } from "../icons/HiChevronRight.tsx";
import { TrackClick, TrackQueryId } from "../islands/Improvements.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../search/utils.ts";
import { Discussions } from "../islands/Discussions.tsx";

import { ALL_REGIONS } from "../search/region.ts";
import { RegionSelector } from "../islands/RegionSelector.tsx";
import {
  decompressCombinedRankingsBase64,
  Ranking,
  SiteRankingSection,
} from "../search/ranking.ts";

export const config = DEFAULT_ROUTE_CONFIG;

type SearchParams = {
  query: string;
  currentPage: number;
  optic: string | undefined;
  selectedRegion: string | undefined;
  safeSearch: boolean;
  siteRankings: Record<SiteRankingSection, Ranking> | undefined;
};

const extractSearchParams = (searchParams: URLSearchParams): SearchParams => {
  const query = searchParams.get("q") ?? "";
  const currentPage = parseInt(searchParams.get("p") ?? "1") || 1;
  const optic = searchParams.get("optic") || void 0;
  const selectedRegion = searchParams.get("gl") || void 0;
  const safeSearch = searchParams.get("ss") == "true";
  const siteRankingsParam = searchParams.get("sr");
  const siteRankings = siteRankingsParam
    ? decompressCombinedRankingsBase64(siteRankingsParam)
    : void 0;

  return {
    query,
    currentPage,
    optic,
    selectedRegion,
    safeSearch,
    siteRankings,
  };
};

export default defineRoute(async (_req, ctx) => {
  injectGlobal`
    .search-content {
      text-rendering: optimizeLegibility;
      font-smooth: always;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
    }
    .text-snippet {
      font-weight: 400;
    }
    .text-snippet > b {
      font-weight: 700;
    }
  `;

  const selected = signal<SelectedAdjust>(null);

  const {
    query,
    currentPage,
    optic,
    selectedRegion,
    safeSearch,
    siteRankings,
  } = extractSearchParams(ctx.url.searchParams);

  const selectedRegionSignal = signal<search.Region>(
    selectedRegion as search.Region,
  );

  if (!query) {
    return Response.redirect(ctx.url.origin);
  }

  const { data } = search.api.search({
    query,
    optic: optic && await fetchRemoteOptic({ opticUrl: optic }),
    page: currentPage - 1,
    safeSearch: safeSearch,
    selectedRegion: ALL_REGIONS.includes(selectedRegion as search.Region)
      ? selectedRegion as search.Region
      : void 0,
    siteRankings,
  });
  const results = await data;

  if (results.type == "bang") {
    return Response.redirect(results.redirectTo);
  }

  const {
    numHits,
    searchDurationSec,
    sidebar,
    hasMoreResults,
    widget,
    displayedAnswer,
    discussions,
  } = match(results).with({ type: "websites" }, (res) => ({
    numHits: res.numHits,
    searchDurationSec: res.searchDurationMs / 1000,
    sidebar: res.sidebar,
    hasMoreResults: res.hasMoreResults,
    widget: res.widget ?? void 0,
    displayedAnswer: res.directAnswer ?? void 0,
    discussions: res.discussions ?? void 0,
  })).exhaustive();

  const numHitsFormatted = numHits.toLocaleString();
  const searchDurationSecFormatted = searchDurationSec.toFixed(2);

  const prevPageSearchParams = match(currentPage > 1).with(true, () => {
    const params = new URLSearchParams(ctx.url.searchParams);
    params.set("p", (currentPage - 1).toString());
    return params;
  }).otherwise(() => {});
  const nextPageSearchParams = match(hasMoreResults).with(true, () => {
    const params = new URLSearchParams(ctx.url.searchParams);
    params.set("p", (currentPage + 1).toString());
    return params;
  }).otherwise(() => {});

  return (
    <>
      <Head>
        <title>{query} - Stract</title>
      </Head>

      <TrackQueryId
        query={query}
        urls={results.type == "websites"
          ? results.webpages.map((wp) => wp.url)
          : []}
      />

      <main class="flex w-full flex-col">
        <Header
          active="Search"
          showDivider={true}
          queryUrlPart={ctx.url.searchParams.toString()}
        />

        <div class="search-content w-screen m-0 grid gap-y-6 pt-4 px-5 md:grid-cols-[minmax(50ch,48rem)_1fr] md:grid-rows-[auto_1fr] md:gap-x-12 md:pl-20 lg:px-36">
          <div class="flex flex-col space-y-5 max-w-2xl">
            <div class="w-full">
              <Searchbar
                defaultQuery={query}
              />
            </div>
            {/* <!-- Stats and settings --> */}
            <div class="mx-auto flex w-full justify-between">
              <div class="flex space-x-2 h-full flex-col justify-center text-sm text-gray-600">
                <p class="h-fit">
                  Found {numHitsFormatted} results in{" "}
                  {searchDurationSecFormatted} seconds
                </p>
              </div>
              <div class="flex space-x-2">
                <OpticSelector
                  defaultOptics={DEFAULT_OPTICS}
                  searchOnChange={true}
                  selected={optic}
                />

                <RegionSelector
                  selectedRegion={selectedRegionSignal}
                />
              </div>
            </div>
          </div>

          <ResultsWebsites
            results={results}
            prevPageUrl={prevPageSearchParams &&
              `/search?${prevPageSearchParams}`}
            nextPageUrl={nextPageSearchParams &&
              `/search?${nextPageSearchParams}`}
            currentPage={currentPage}
            selected={selected}
            widget={widget}
            displayedAnswer={displayedAnswer}
            discussions={discussions}
          />

          {sidebar &&
            (
              <div class="row-start-2 justify-center md:pt-10 md:col-start-2 md:row-span-2 md:row-start-1 max-w-[90vw] mx-auto">
                <Sidebar sidebar={sidebar} />
              </div>
            )}
        </div>

        <SearchResultAdjustModal query={query} selected={selected} />
      </main>

      <Footer />
    </>
  );
});

/**
 * Fetces the given `opticUrl` if allowed. The rules for which are allowed
 * should consider potentially malicious URLs such as `file://` or
 * internal/local IP addresses.
 */
const fetchRemoteOptic = async (opts: { opticUrl: string }) => {
  if (opts.opticUrl.startsWith("file://")) return void 0;
  const response = await fetch(opts.opticUrl);
  return await response.text();
};

const ResultsWebsites = (
  {
    results,
    currentPage,
    prevPageUrl,
    nextPageUrl,
    selected,
    widget,
    displayedAnswer,
    discussions,
  }: {
    results: search.WebsitesResult;
    prevPageUrl: string | void;
    nextPageUrl: string | void;
    currentPage: number;
    selected: Signal<SelectedAdjust>;
    widget?: search.Widget;
    displayedAnswer?: search.DisplayedAnswer;
    discussions?: search.Webpage[];
  },
) => (
  <div class="col-start-1 flex min-w-0 max-w-2xl flex-col space-y-10">
    {match(results.spellCorrectedQuery).with(
      P.not(P.nullish),
      (corrected) => (
        <div>
          Did you mean:{" "}
          <a
            class="font-medium"
            href={`/search?q=${encodeURIComponent(corrected.raw)}`}
            dangerouslySetInnerHTML={{ __html: corrected.highlighted }}
          />
        </div>
      ),
    ).otherwise(() => null)}

    {widget && <Widget widget={widget} />}
    {displayedAnswer && <DisplayedAnswer displayedAnswer={displayedAnswer} />}

    {results.webpages.slice(0, 4).map((item, idx) => (
      <Webpage
        key={item.url}
        click={idx.toString()}
        item={item}
        selected={selected}
      />
    ))}
    {discussions && <Discussions discussions={discussions} />}
    {results.webpages.slice(4).map((item, idx) => (
      <Webpage
        key={item.url}
        click={idx.toString()}
        item={item}
        selected={selected}
      />
    ))}

    <div class="flex justify-center">
      <div class="grid items-center justify-center grid-cols-[repeat(3,auto)] gap-2">
        <div class="row-start-1 col-start-2">
          Page {currentPage}
        </div>
        {[
          { url: prevPageUrl, Icon: HiChevronLeft },
          { url: nextPageUrl, Icon: HiChevronRight },
        ].map(({ url, Icon }) => (
          <a href={url || void 0}>
            <Icon
              class={tx(
                "w-6",
                url ? "text-brand-500 hover:text-brand-600" : "text-gray-500",
              )}
            />
          </a>
        ))}
      </div>
    </div>
  </div>
);

const Webpage = (
  { item, selected, click }: {
    item: search.Webpage;
    selected: Signal<SelectedAdjust>;
    click: string;
  },
) => (
  <div class="flex w-full">
    <div class="flex min-w-0 grow flex-col space-y-1">
      <div class="flex min-w-0">
        <div class="flex min-w-0 grow flex-col space-y-1">
          <div class="flex items-center text-sm">
            <TrackClick
              click={click}
              class="truncate text-gray-800 dark:text-brand-100 hover:no-underline improvement-on-click max-w-[calc(100%-100px)]"
              href={item.url}
            >
              {item.prettyUrl}
            </TrackClick>
          </div>
          <TrackClick
            click={click}
            class="text-blue-800 dark:text-blue-500 visited:text-purple-800 dark:visited:text-purple-500 sr-title-link truncate text-xl font-medium improvement-on-click max-w-[calc(100%-30px)]"
            title={item.title}
            href={item.url}
          >
            {item.title}
          </TrackClick>
        </div>
        <SearchResultAdjust item={item} selected={selected} />
      </div>
      <div class="text-sm text-snippet dark:text-stone-400">
        <Snippet item={item} />
      </div>
    </div>
  </div>
);

const Widget = ({ widget }: { widget: search.Widget }) =>
  match(widget)
    .with(
      { type: "calculator" },
      ({ value }) => <CalculatorWidget calculation={value} />,
    )
    .exhaustive();

const CalculatorWidget = (
  { calculation }: {
    calculation: search.Widget extends { type: "calculator"; value: infer T }
      ? T
      : never;
  },
) => (
  <div class="flex flex-col items-end rounded-xl border p-5">
    <div class="flex w-fit text-xs text-gray-500">
      {calculation.input} =
    </div>
    <div class="flex w-fit text-3xl font-bold">
      {calculation.result}
    </div>
  </div>
);

const DisplayedAnswer = (
  { displayedAnswer }: { displayedAnswer: search.DisplayedAnswer },
) => (
  <div class="flex flex-col border-b">
    <div class="answer">{displayedAnswer.answer}</div>
    <div
      class="mb-5 text-sm [&>b]:font-bold"
      dangerouslySetInnerHTML={{
        __html: displayedAnswer.snippet,
      }}
    />
    <div class="flex flex-col">
      <a
        class="inline-block truncate text-sm text-gray-800 hover:no-underline m-0 p-0 max-w-[calc(100%-30px)]"
        href={displayedAnswer.url}
      >
        {displayedAnswer.prettyUrl}
      </a>
      <div class="answer-title">
        <a
          class="inline-block truncate text-xl font-medium text-blue-800 visited:text-purple-800 max-w-[calc(100%-30px)]"
          href={displayedAnswer.url}
        >
          {displayedAnswer.title}
        </a>
      </div>
    </div>
  </div>
);

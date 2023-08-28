import { RouteContext } from "$fresh/server.ts";
import { Searchbar } from "../islands/Searchbar.tsx";
import { injectGlobal } from "https://esm.sh/@twind/core@1.1.3";
import * as search from "../search/index.ts";
import { match, P } from "ts-pattern";
import { Header } from "../components/Header.tsx";
import { OpticSelector } from "../islands/OpticsSelector.tsx";
import { Select } from "../components/Select.tsx";
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

export const config = DEFAULT_ROUTE_CONFIG;

export default async function Search(_req: Request, ctx: RouteContext) {
  const selected = signal<SelectedAdjust>(null);

  const query = ctx.url.searchParams.get("q") ?? "";
  const currentPage = parseInt(ctx.url.searchParams.get("p") ?? "1") || 1;
  const optic = ctx.url.searchParams.get("optic") ?? void 0;
  const selectedRegion = ctx.url.searchParams.get("gl") ?? void 0;
  const safeSearch = ctx.url.searchParams.get("ss") == "true";

  if (!query) {
    return Response.redirect(ctx.url.origin);
  }

  const { data } = search.api.search({
    query,
    optic: optic && await fetchRemoteOptic({ opticUrl: optic }),
    page: currentPage - 1,
    safeSearch: safeSearch,
    selectedRegion: search.ALL_REGIONS.includes(selectedRegion as search.Region)
      ? selectedRegion as search.Region
      : void 0,
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
        <Header active="Search" showDivider={true} />

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
                  Found {numHits} results in {searchDurationSec} seconds
                </p>
              </div>
              <div class="flex space-x-2">
                <OpticSelector
                  defaultOptics={DEFAULT_OPTICS}
                  searchOnChange={true}
                />
                <div class="select-region flex h-full flex-col justify-center">
                  <Select
                    form="searchbar-form"
                    id="region-selector"
                    name="gl"
                  >
                    <option>
                      All Languages
                    </option>
                    {search.ALL_REGIONS.slice(1).map((region) => (
                      <option
                        value={region}
                        selected={region == selectedRegion}
                      >
                        {region}
                      </option>
                    ))}
                  </Select>
                </div>
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
}

injectGlobal`
.search-content {
  text-rendering: optimizeLegibility;
  font-smooth: always;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

.change-page-inactive {
  @apply w-6 text-gray-500;
}

.change-page-active {
  @apply w-6 text-brand/80 hover:text-brand;
}

.text-snippet {
  font-weight: 400;
}
.text-snippet > b {
  font-weight: 700;
}
`;

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

    <div class="flex w-full items-center justify-center">
      {prevPageUrl
        ? (
          <a href={prevPageUrl}>
            <HiChevronLeft class="change-page-active" />
          </a>
        )
        : <HiChevronLeft class="change-page-inactive" />}
      <div class="mx-2">
        Page {currentPage}
      </div>
      {nextPageUrl
        ? (
          <a href={nextPageUrl}>
            <HiChevronRight class="change-page-active" />
          </a>
        )
        : <HiChevronRight class="change-page-inactive" />}
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
              class="truncate text-gray-800 hover:no-underline improvement-on-click max-w-[calc(100%-100px)]"
              href={item.url}
              data-idx="{{ loop.index0 }}"
            >
              {item.prettyUrl}
            </TrackClick>
          </div>
          <TrackClick
            click={click}
            class="text-blue-800 visited:text-purple-800 sr-title-link truncate text-xl font-medium improvement-on-click max-w-[calc(100%-30px)]"
            title={item.title}
            href={item.url}
            data-idx="{{ loop.index0 }}"
          >
            {item.title}
          </TrackClick>
        </div>
        <SearchResultAdjust item={item} selected={selected} />
      </div>
      <div class="text-sm text-snippet">
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

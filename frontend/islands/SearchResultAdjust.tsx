import { Signal } from "@preact/signals";
import * as search from "../search/index.ts";
import { useEffect } from "preact/hooks";
import { IS_BROWSER } from "$fresh/runtime.ts";
import { JSX } from "preact";
import {
  RankingSignal,
  useRanking,
  useSaveRanking,
} from "../search/ranking.ts";
import { match } from "ts-pattern";
import { summarySignals } from "../search/summary.ts";
import { updateStorageSignal } from "../search/utils.ts";
import { HiAdjustmentsVerticalOutline } from "../icons/HiAdjustmentsVerticalOutline.tsx";
import { HiHandThumbUpMini } from "../icons/HiHandThumbUpMini.tsx";
import { HiHandThumbDownMini } from "../icons/HiHandThumbDownMini.tsx";
import { HiNoSymbolMini } from "../icons/HiNoSymbolMini.tsx";
import { tx } from "https://esm.sh/@twind/core@1.1.3";
import { Button } from "../components/Button.tsx";

export type SelectedAdjust =
  | { button: HTMLElement; item: search.Webpage }
  | null;

export default function SearchResultAdjust(
  { item, selected }: {
    item: search.Webpage;
    selected: Signal<SelectedAdjust>;
  },
) {
  return (
    <button
      class={tx`
      adjust-button hidden min-w-fit items-center justify-center md:flex w-5 hover:cursor-pointer bg-transparent noscript:hidden
      text-gray-700/50 hover:text-gray-700
      dark:(text-stone-400 hover:text-stone-300)
      `}
      data-site={item.site}
      data-url={item.url}
      onClick={(e) => {
        e.stopPropagation();
        selected.value = { button: e.currentTarget as HTMLElement, item };
      }}
    >
      <HiAdjustmentsVerticalOutline class="w-6" />
    </button>
  );
}

export const SearchResultAdjustModal = (
  { query, selected }: { query: string; selected: Signal<SelectedAdjust> },
) => {
  useEffect(() => {
    const listener = () => {
      selected.value = null;
    };
    document.addEventListener("click", listener);
    return () => {
      document.removeEventListener("click", listener);
    };
  }, [selected]);

  const rect = selected.value
    ? selected.value.button.getBoundingClientRect()
    : void 0;
  const rankingModalHeight = 0;

  if (!IS_BROWSER) return null;

  const liked = useRanking("liked");
  const disliked = useRanking("disliked");
  const blocked = useRanking("blocked");

  useSaveRanking(liked);
  useSaveRanking(disliked);
  useSaveRanking(blocked);

  return (
    <div
      class={tx`
        absolute h-fit w-52 origin-left flex-col items-center overflow-hidden rounded-lg py-5 px-2 text-sm drop-shadow-md -translate-y-1/2
        border dark:border-stone-700
        bg-white dark:bg-stone-800
        ${rect ? "flex transition-all scale-1" : "scale-0"}
      `}
      id="modal"
      style={rect && {
        left: rect.left + rect.width + 5 + "px",
        top: (rect.top + rect.bottom) / 2 + document.documentElement.scrollTop -
          rankingModalHeight / 2 + "px",
      }}
      onClick={(e) => {
        e.stopPropagation();
      }}
    >
      <div>
        <h2 class="w-fit text-center">
          Do you like results from{" "}
          {selected.value ? selected.value.item.domain : "EXAMPLE.com"}?
        </h2>
        <div class="flex space-x-1.5 pt-2 justify-center">
          <AdjustButton
            base="brand"
            ranking={liked}
            others={[disliked, blocked]}
            selected={selected}
            form="searchbar-form"
          >
            <HiHandThumbUpMini class="w-4" />
          </AdjustButton>
          <AdjustButton
            base="amber"
            ranking={disliked}
            others={[liked, blocked]}
            selected={selected}
            form="searchbar-form"
          >
            <HiHandThumbDownMini class="w-4" />
          </AdjustButton>
          <AdjustButton
            base="red"
            ranking={blocked}
            others={[liked, disliked]}
            selected={selected}
            form="searchbar-form"
          >
            <HiNoSymbolMini class="w-4" />
          </AdjustButton>
        </div>
      </div>
      <div class="mt-4 flex justify-center">
        <Button
          pale
          onClick={() => {
            const item = selected.value?.item;
            if (!item) return;

            search.api.summarize({ query, url: item.url }, (e) => {
              summarySignals.value = match(e)
                .with({ type: "begin" }, () => ({
                  ...summarySignals.value,
                  [item.url]: { inProgress: true, data: "" },
                }))
                .with({ type: "content" }, ({ data }) => ({
                  ...summarySignals.value,
                  [item.url]: {
                    inProgress: true,
                    data: (summarySignals.value[item.url]?.data ?? "") + data,
                  },
                }))
                .with({ type: "done" }, () => ({
                  ...summarySignals.value,
                  [item.url]: {
                    inProgress: false,
                    data: (summarySignals.value[item.url]?.data ?? ""),
                  },
                })).exhaustive();
            });
          }}
        >
          Summarize Result
        </Button>
      </div>
    </div>
  );
};

function AdjustButton(
  { base, ranking, others, selected, children, ...props }: {
    base: string;
    ranking: RankingSignal;
    others: RankingSignal[];
    selected: Signal<SelectedAdjust>;
  } & Omit<JSX.HTMLAttributes<HTMLButtonElement>, "selected">,
) {
  const active = ranking.signal.value.data.includes(
    selected.value?.item.domain ?? " selected",
  );

  return (
    <Button
      {...props}
      class="!px-2 border text-opacity-80"
      pale={!active}
      base={active ? base : "gray"}
      onClick={() => {
        const domain = selected.value?.item.domain;
        if (!domain) return;

        if (active) {
          updateStorageSignal(ranking.signal, (sites) =>
            sites.filter((s) => s != domain));
        } else {
          updateStorageSignal(ranking.signal, (sites) => [...sites, domain]);
        }
        for (const other of others) {
          updateStorageSignal(other.signal, (sites) =>
            sites.filter((s) =>
              s != domain
            ));
        }
      }}
    >
      {children}
    </Button>
  );
}

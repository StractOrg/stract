import { signal } from "@preact/signals";
import { IS_BROWSER } from "$fresh/runtime.ts";
import { useEffect } from "preact/hooks";
import { allowStatsSignal } from "../search/preferences.ts";
import { useSyncSignalWithLocalStorage } from "../search/utils.ts";
import { JSX } from "preact/jsx-runtime";
import * as search from "../search/index.ts";

const queryIdSignal = signal<string | null>(null);

export const TrackQueryId = (
  { query, urls }: { query: string; urls: string[] },
) => {
  useSyncSignalWithLocalStorage(allowStatsSignal);

  if (!IS_BROWSER) return null;
  if (!allowStatsSignal.value) return null;

  useEffect(() => {
    const { data, abort } = search.api.queryId({
      query,
      urls,
    });
    data.then((qid) => queryIdSignal.value = qid);
    return abort;
  }, [query, urls.join("~~")]);

  return null;
};

const improvementSentSignal = signal<Record<string, boolean>>({});

export const TrackClick = (
  { click, onClick, ...props }:
    & { click: string }
    & JSX.HTMLAttributes<HTMLAnchorElement>,
) => {
  const allowStats = allowStatsSignal.value;
  const queryId = queryIdSignal.value;
  const alreadySent = click in improvementSentSignal.value;

  if (!IS_BROWSER || !allowStats || typeof queryId != "string" || alreadySent) {
    return <a onClick={onClick} {...props} />;
  }

  return (
    <a
      onClick={(event) => {
        improvementSentSignal.value = {
          ...improvementSentSignal.value,
          [click]: true,
        };
        search.api.sendImprovementClick({ queryId, click });

        onClick?.(event);
      }}
      {...props}
    />
  );
};

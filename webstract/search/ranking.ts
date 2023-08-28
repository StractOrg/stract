import LZString from "npm:lz-string";
import {
  StorageSignal,
  storageSignal,
  useSyncSignalWithLocalStorage,
} from "./utils.ts";

export type SiteRankingSection = "liked" | "disliked" | "blocked";
export const SITE_RANKING_SECTIONS: readonly SiteRankingSection[] = [
  "liked",
  "disliked",
  "blocked",
] as const;

export type RankedSite = string;

export type Ranking = RankedSite[];
export type RankingSignal = {
  section: SiteRankingSection;
  signal: StorageSignal<Ranking>;
};

const rankingLocalStorageKey = (section: SiteRankingSection) =>
  `SiteRanking-${section}`;
const rankingSignals = SITE_RANKING_SECTIONS.map<RankingSignal>((section) => ({
  section,
  signal: storageSignal(
    rankingLocalStorageKey(section),
    [],
  ),
}));

export const useRanking = (section: SiteRankingSection): RankingSignal =>
  rankingSignals.find((r) => r.section == section)!;
export const useSaveRanking = (ranking: RankingSignal) =>
  useSyncSignalWithLocalStorage(ranking.signal);

export const useAllRankings = (): [SiteRankingSection, RankingSignal][] =>
  SITE_RANKING_SECTIONS.map((sec) => [sec, useRanking(sec)]);
const generateCombinedRankings = (
  rankings: [SiteRankingSection, Ranking][],
) => {
  const result: Record<SiteRankingSection, Ranking> = {
    liked: [],
    disliked: [],
    blocked: [],
  };

  for (const [section, sites] of rankings) {
    result[section] ??= [];
    result[section] = [...result[section], ...sites];
  }

  return result;
};
export const generateCombinedRankingsBase64 = (
  rankings: [SiteRankingSection, Ranking][],
) =>
  LZString.compressToBase64(JSON.stringify(generateCombinedRankings(rankings)));

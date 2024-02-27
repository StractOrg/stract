import LZString from 'lz-string';

export enum Ranking {
  LIKED = 'liked',
  DISLIKED = 'disliked',
  BLOCKED = 'blocked'
}

export type SiteRankings = Record<string, Ranking | undefined>;
export type RankedSites = Record<Ranking, string[]>;

export const rankingsToRanked = (rankings: SiteRankings): RankedSites => {
  const result: RankedSites = {
    liked: [],
    disliked: [],
    blocked: [],
  };

  for (const [site, ranking] of Object.entries(rankings)) {
    if (ranking) result[ranking].push(site);
  }

  return result;
};

export const rankedToRankings = (ranked: RankedSites): SiteRankings => {
  const result: SiteRankings = {};

  for (const [_, ranking] of Object.entries(Ranking)) {
    for (const site of ranked[ranking]) {
      result[site] = ranking;
    }
  }

  return result;
};

export const compressRanked = (ranked: RankedSites): string =>
  LZString.compressToBase64(JSON.stringify(ranked));
export const decompressRanked = (compressedRanked: string): RankedSites =>
  JSON.parse(LZString.decompressFromBase64(compressedRanked));

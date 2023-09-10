import LZString from 'lz-string';

export type Ranking = 'liked' | 'disliked' | 'blocked';
export type SiteRakings = Record<string, Ranking | undefined>;
export type RankedSites = Record<Ranking, string[]>;

export const rankingsToRanked = (rankings: SiteRakings): RankedSites => {
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
export const rankedToRankings = (ranked: RankedSites): SiteRakings => {
  const result: SiteRakings = {};

  for (const ranking of ['liked', 'disliked', 'blocked'] as const) {
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

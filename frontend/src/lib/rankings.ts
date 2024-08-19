export type Ranking = 'liked' | 'disliked' | 'blocked';
export const Rankings = ['liked', 'disliked', 'blocked'] as Ranking[];

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

  for (const ranking of Rankings) {
    for (const site of ranked[ranking]) {
      result[site] = ranking;
    }
  }

  return result;
};

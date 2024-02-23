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

// Map regexs to their respective rankings
const reToRankMap = {
  [Ranking.LIKED]: /Like\(\s*Site\s*\(\s*"(\S*)"\s*\)\s*\);/g,
  [Ranking.DISLIKED]: /Dislike\(\s*Site\s*\(\s*"(\S*)"\s*\)\s*\);/g,
  [Ranking.BLOCKED]: /Rule\s*{\s*Matches\s*{\s*Site\("\|(\S*)\|"\),\s*},\s*Action\(Discard\)\s*};/g
}

export const importedOpticToRankings = (optic: string): SiteRankings => {
  const rankedSites: RankedSites = {
    liked: [],
    disliked: [],
    blocked: [],
  };

  for(const [_, ranking] of Object.entries(Ranking)){
    let re = reToRankMap[ranking]
    let match: RegExpExecArray | null

    do {
      match = re.exec(optic);
      if (match) {
        rankedSites[ranking].push(match[1])
      }
    } while (match)
  }

  return rankedToRankings(rankedSites)
}

export const compressRanked = (ranked: RankedSites): string =>
  LZString.compressToBase64(JSON.stringify(ranked));
export const decompressRanked = (compressedRanked: string): RankedSites =>
  JSON.parse(LZString.decompressFromBase64(compressedRanked));

import type { Region, Widget, DisplayedSidebar, DisplayedWebpage, WebsitesResult, BangHit } from './api';
import { decompressRanked, type RankedSites } from './rankings';

export type SearchParams = {
  query: string;
  currentPage: number;
  optic: string | undefined;
  selectedRegion: Region | undefined;
  safeSearch: boolean;
  compressedhost_rankings: string | null;
  host_rankings: RankedSites | undefined;
};

export type SearchResults = 
  | (WebsitesResult & {
    type: 'websites';
    widget?: Widget;
    sidebar?: DisplayedSidebar;
    discussions?: DisplayedWebpage[];
  })
  | (BangHit & {
    type: 'bang';
  });

export const extractSearchParams = (searchParams: URLSearchParams | FormData): SearchParams => {
  const query = (searchParams.get('q') as string | undefined) ?? '';
  const currentPage = parseInt((searchParams.get('p') as string | undefined) ?? '1') || 1;
  const optic = (searchParams.get('optic') as string | undefined) || void 0;
  const selectedRegion = ((searchParams.get('gl') as string | undefined) || void 0) as
    | Region
    | undefined;
  const safeSearch = (searchParams.get('ss') as string | undefined) == 'true';
  const compressedhost_rankings = (searchParams.get('sr') as string | undefined) || null;
  const host_rankings = compressedhost_rankings ? decompressRanked(compressedhost_rankings) : void 0;

  return {
    query,
    currentPage,
    optic,
    selectedRegion,
    safeSearch,
    compressedhost_rankings,
    host_rankings,
  };
};

export const discussionsOptic: string = `DiscardNonMatching;

Rule {
    Matches {
        Schema("QAPage"),
    }
};

Rule {
    Matches {
        Schema("DiscussionForumPosting"),
    }
};

Rule {
    Matches {
        Domain("|reddit.com|"),
        Url("comments"),
    }
};

Rule {
    Matches {
        Site("|lemmy.world|")
    }
};

Rule {
    Matches {
        Site("|lemmy.ml|")
    }
};

Rule {
    Matches {
        Site("|sh.itjust.works|")
    }
};`;
import {
  type Region,
  type Widget,
  type DisplayedSidebar,
  type DisplayedWebpage,
  type WebsitesResult,
  type BangHit,
  type HighlightedSpellCorrection,
  type ApiOptions,
  api,
} from './api';
import { fetchRemoteOptic } from './optics';
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
      spellCorrection?: HighlightedSpellCorrection;
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
  const host_rankings = compressedhost_rankings
    ? decompressRanked(compressedhost_rankings)
    : void 0;

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

const discussionsOptic: string = `DiscardNonMatching;

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

export const search = async (params: SearchParams, options: ApiOptions) => {
  const { data: websitesReq } = api.search(
    {
      query: params.query,
      page: params.currentPage - 1,
      safeSearch: params.safeSearch,
      optic: params.optic && (await fetchRemoteOptic({ opticUrl: params.optic, fetch })),
      selectedRegion: params.selectedRegion,
      hostRankings: params.host_rankings,
      countResults: true,
    },
    options,
  );

  const { data: widgetReq } =
    params.currentPage == 1
      ? api.searchWidget(
          {
            query: params.query,
          },
          options,
        )
      : { data: undefined };

  const { data: sidebarReq } =
    params.currentPage == 1
      ? api.searchSidebar(
          {
            query: params.query,
          },
          options,
        )
      : { data: undefined };

  const { data: discussionsReq } =
    params.currentPage == 1 && params.optic == undefined
      ? api.search(
          {
            query: params.query,
            optic: discussionsOptic,
            numResults: 10,
            safeSearch: params.safeSearch,
            selectedRegion: params.selectedRegion,
            hostRankings: params.host_rankings,
            countResults: false,
          },
          options,
        )
      : { data: undefined };

  const { data: spellcheckReq } = api.searchSpellcheck({ query: params.query }, options);

  const [websites, widget, sidebar, discussionsRes, spellCorrection] = await Promise.all([
    websitesReq,
    widgetReq,
    sidebarReq,
    discussionsReq,
    spellcheckReq,
  ]);
  const discussions = discussionsRes?.type == 'websites' ? discussionsRes.webpages : undefined;

  const results: SearchResults =
    websites.type == 'websites'
      ? {
          ...websites,
          widget,
          sidebar,
          discussions,
          spellCorrection,
        }
      : {
          ...websites,
        };

  return results;
};

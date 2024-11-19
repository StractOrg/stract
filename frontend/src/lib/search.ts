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
import { type RankedSites } from './rankings';

export type SearchParams = {
  query: string;
  currentPage: number;
  optic: string | undefined;
  selectedRegion: Region | undefined;
  safeSearch: boolean;
  hostRankings: RankedSites | undefined;
  showRankingSignals?: boolean;
};

export type SearchResults =
  | (WebsitesResult & {
      _type: 'websites';
      spellCorrection?: HighlightedSpellCorrection;
      widget?: Widget;
      sidebar?: DisplayedSidebar;
      discussions?: DisplayedWebpage[];
    })
  | (BangHit & {
      _type: 'bang';
    });

export const extractSearchParams = (searchParams: URLSearchParams | FormData): SearchParams => {
  const query = (searchParams.get('q') as string | undefined) ?? '';
  const currentPage = parseInt((searchParams.get('p') as string | undefined) ?? '1') || 1;
  const optic = (searchParams.get('optic') as string | undefined) || void 0;
  const selectedRegion = ((searchParams.get('gl') as string | undefined) || void 0) as
    | Region
    | undefined;
  const safeSearch = (searchParams.get('ss') as string | undefined) == 'true';

  return {
    query,
    currentPage,
    optic,
    selectedRegion,
    safeSearch,
    hostRankings: undefined,
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
      hostRankings: params.hostRankings,
      returnRankingSignals: params.showRankingSignals,
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
            hostRankings: params.hostRankings,
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
  const discussions = discussionsRes?._type == 'websites' ? discussionsRes.webpages : undefined;

  const results: SearchResults =
    websites._type == 'websites'
      ? {
          ...websites,
          widget: widget ?? undefined,
          sidebar: sidebar ?? undefined,
          discussions,
          spellCorrection: spellCorrection ?? undefined,
        }
      : {
          ...websites,
        };

  return results;
};

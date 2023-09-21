import type { Region } from "./api";
import { decompressRanked, type RankedSites } from "./rankings";

export type SearchParams = {
    query: string;
    currentPage: number;
    optic: string | undefined;
    selectedRegion: Region | undefined;
    safeSearch: boolean;
    compressedSiteRankings: string | null;
    siteRankings: RankedSites | undefined;
  };
  
  export const extractSearchParams = (searchParams: URLSearchParams | FormData): SearchParams => {
    const query = searchParams.get('q') as string | undefined ?? '';
    const currentPage = parseInt(searchParams.get('p') as string | undefined ?? '1') || 1;
    const optic = searchParams.get('optic') as string | undefined || void 0;
    const selectedRegion = (searchParams.get('gl') as string | undefined || void 0) as Region | undefined;
    const safeSearch = searchParams.get('ss') as string | undefined == 'true';
    const compressedSiteRankings = searchParams.get('sr') as string | undefined || null;
    const siteRankings = compressedSiteRankings ? decompressRanked(compressedSiteRankings) : void 0;
  
    return {
      query,
      currentPage,
      optic,
      selectedRegion,
      safeSearch,
      compressedSiteRankings,
      siteRankings,
    };
  };
  
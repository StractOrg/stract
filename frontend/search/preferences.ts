import { storageSignal } from "./utils.ts";

const ALLOW_STATS_LOCAL_STORAGE_KEY = "allowStats";
export const allowStatsSignal = storageSignal<boolean>(
  ALLOW_STATS_LOCAL_STORAGE_KEY,
  true,
);

const SAFE_SEARCH_LOCAL_STORAGE_KEY = "safeSearch";
export const safeSearchSignal = storageSignal<boolean>(
  SAFE_SEARCH_LOCAL_STORAGE_KEY,
  false,
);

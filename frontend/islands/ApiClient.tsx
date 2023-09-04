import { useEffect } from "preact/hooks";
import { API_BASE } from "../search/index.ts";

export const ApiClient = ({ apiBase }: { apiBase: string }) => {
  useEffect(() => {
    API_BASE.value = apiBase;
  }, [apiBase]);
  return null;
};

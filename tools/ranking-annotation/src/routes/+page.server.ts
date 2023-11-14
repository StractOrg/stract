import { getQueries } from "$lib/db";

export const load = async () => {
  const queries = getQueries();

  return { queries };
};

import { getNextQuery, getPreviousQuery, getQuery, getSearchResults, saveSearchResults } from "$lib/db";
import type { Webpage } from "$lib/webpage";
import { asSimpleWebpage } from "$lib/webpage";
import { redirect } from "@sveltejs/kit";

const search = async (query: string) => {
  return await fetch(`https://trystract.com/beta/api/search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      accept: "application/json",
    },
    body: JSON.stringify({
      query,
      numResults: 100,
      returnRankingSignals: true,
    }),
  })
    .then((res) => {
      return res.json();
    })
    .then((d) => d["webpages"] as Webpage[]);
};

export const load = async ({ params }) => {
  const { slug } = params;
  const qid = slug;


  if (!getQuery(qid)) {
    throw redirect(301, "/");
  }
  const query = getQuery(qid)!;

  const previousQuery = getPreviousQuery(qid);
  const nextQuery = getNextQuery(qid);

  let searchResults = getSearchResults(query.qid);

  if (searchResults.length === 0) {
    const webpages = (await search(query.query)).map((w) => asSimpleWebpage(w));
    
    searchResults = webpages.map((w, i) => ({
      id: `${query.qid}-${w.url}`,
      origRank: i,
      annotation: null,
      webpage: w,
    }));

    saveSearchResults(query.qid, searchResults);
  }

  // sort results by annotation and then origRank
  // annotation is 0-4 where 4 is best, null is unannotated
  searchResults.sort((a, b) => {
    if ((a.annotation === null && b.annotation === null) || a.annotation === b.annotation) {
      return a.origRank - b.origRank;
    } else if (a.annotation === null) {
      return 1;
    } else if (b.annotation === null) {
      return -1;
    } else {
      return b.annotation - a.annotation;
    }
  });

  return { query, searchResults, previousQuery, nextQuery };
};
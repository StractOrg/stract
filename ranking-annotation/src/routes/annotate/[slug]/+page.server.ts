import { getQuery, getSearchResults, saveSearchResults } from "$lib/db";
import type { Webpage } from "$lib/webpage";
import { asSimpleWebpage } from "$lib/webpage";

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

  const query = getQuery(qid);
  let searchResults = getSearchResults(query.qid);

  if (searchResults.length === 0) {
    const webpages = (await search(query.query)).map((w) => asSimpleWebpage(w));
    
    searchResults = webpages.map((w, i) => ({
      origRank: i,
      annotatedRank: null,
      webpage: w,
    }));

    saveSearchResults(query.qid, searchResults);
  }

  // sort results by annotatedRank and then origRank
  searchResults.sort((a, b) => {
    if (a.annotatedRank === null && b.annotatedRank === null) {
      return a.origRank - b.origRank;
    } else if (a.annotatedRank === null) {
      return 1;
    } else if (b.annotatedRank === null) {
      return -1;
    } else {
      return a.annotatedRank - b.annotatedRank;
    }
  });

  return { query, searchResults };
};
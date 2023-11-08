import Database from "better-sqlite3";
import { getDataset } from "./dataset";
import type { SimpleWebpage } from "./webpage";

const db = new Database("../data/ranking-annotation.sqlite", {
  fileMustExist: false,
});

setupDB();

function setupDB() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS queries (
	  qid UUID PRIMARY KEY DEFAULT (HEX(RANDOMBLOB(16))),
      query TEXT NOT NULL UNIQUE
    );
  `);

  db.exec(`
  CREATE TABLE IF NOT EXISTS search_results (
	qid UUID NOT NULL,
	url TEXT NOT NULL,
	orig_rank INTEGER NOT NULL,
	webpage_json TEXT NOT NULL,
	anotated_rank INTEGER,
	PRIMARY KEY (qid, url)
  );
  `);

  const data = getDataset()!;

  const insertQuery = db.prepare(`
        INSERT OR IGNORE INTO queries (query)
        VALUES (@query)
    `);

  for (const query of data) {
    insertQuery.run({ query });
  }

  return db;
}

export type Query = {
  qid: string;
  query: string;
  annotated: boolean;
};

export function getQueries(): Query[] {
  const query = db.prepare(`
		SELECT qid, query, EXISTS (
			SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.anotated_rank IS NOT NULL
		) AS annotated
		FROM queries
	`);

  return query.all() as Query[];
}

export type SearchResult = {
  origRank: number;
  webpage: SimpleWebpage;
  annotatedRank: number | null;
};

export function getSearchResults(qid: string): SearchResult[] {
  const queryResults = db.prepare(`
		SELECT url, orig_rank as origRank, webpage_json, anotated_rank AS annotatedRank
		FROM search_results
		WHERE qid = @qid
	`);

  const res = queryResults.all({ qid });

  return res.map((r: any) => ({
    origRank: r.origRank,
    annotatedRank: r.annotatedRank,
    webpage: JSON.parse(r.webpage_json),
  }));
}

export const saveSearchResults = (qid: string, searchResults: SearchResult[]) => {
  const insertSearchResults = db.prepare(`
		INSERT INTO search_results (qid, url, orig_rank, webpage_json)
		VALUES (@qid, @url, @origRank, @webpageJson)
	`);

  for (const searchResult of searchResults) {
    insertSearchResults.run({
      qid,
      url: searchResult.webpage.url,
      origRank: searchResult.origRank,
      webpageJson: JSON.stringify(searchResult.webpage),
    });
  }
}

export function getQuery(qid: String): Query {
  const query = db.prepare(`
		SELECT qid, query, EXISTS (
			SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.anotated_rank IS NOT NULL
		) AS annotated
		FROM queries
		WHERE qid = @qid
	`);

  return query.get({ qid }) as Query;
}

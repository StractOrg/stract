import Database from "better-sqlite3";
import { getDataset } from "./dataset";
import type { SimpleWebpage } from "./webpage";

const db = new Database("../../data/ranking-annotation.sqlite", {
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
	annotation INTEGER,
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
			SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation IS NOT NULL
		) AS annotated
		FROM queries ORDER BY qid
	`);

  return query.all() as Query[];
}

export function getNextQuery(qid: string): Query | undefined {
  const query = db.prepare(`
    SELECT qid, query, EXISTS (
      SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation IS NOT NULL
    ) AS annotated
    FROM queries
    WHERE qid > @qid
    ORDER BY qid
    LIMIT 1
  `);

  const res = query.get({ qid });

  return res as Query | undefined;
}

export function getPreviousQuery(qid: string): Query | undefined {
  const query = db.prepare(`
    SELECT qid, query, EXISTS (
      SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation IS NOT NULL
    ) AS annotated
    FROM queries
    WHERE qid < @qid
    ORDER BY qid DESC
    LIMIT 1
  `);

  const res = query.get({ qid });

  return res as Query | undefined;
}

export type SearchResult = {
  id: string;
  origRank: number;
  webpage: SimpleWebpage;
  annotation: number | null;
};

export function getSearchResults(qid: string): SearchResult[] {
  const queryResults = db.prepare(`
		SELECT url, orig_rank as origRank, webpage_json, annotation
		FROM search_results
		WHERE qid = @qid
	`);

  const res = queryResults.all({ qid });

  return res.map((r: any) => ({
    id: `${qid}-${r.url}`,
    origRank: r.origRank,
    annotation: r.annotation,
    webpage: JSON.parse(r.webpage_json),
  }));
}

export const saveSearchResults = (qid: string, searchResults: SearchResult[]) => {
  const upsertSearchResults = db.prepare(`
    INSERT INTO search_results (qid, url, orig_rank, webpage_json, annotation)
    VALUES (@qid, @url, @origRank, @webpageJson, @annotation)
    ON CONFLICT(qid, url) DO UPDATE SET
    annotation = excluded.annotation
  `);

  for (const searchResult of searchResults) {
    upsertSearchResults.run({
      qid,
      url: searchResult.webpage.url,
      origRank: searchResult.origRank,
      webpageJson: JSON.stringify(searchResult.webpage),
      annotation: searchResult.annotation,
    });
  }
}

export function getQuery(qid: String): Query | undefined {
  const query = db.prepare(`
		SELECT qid, query, EXISTS (
			SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation IS NOT NULL
		) AS annotated
		FROM queries
		WHERE qid = @qid
	`);
  const res = query.get({ qid });

  return res as Query | undefined;
}

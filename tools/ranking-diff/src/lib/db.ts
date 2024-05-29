import type { Experiment, Query } from '$lib';
import Database from 'better-sqlite3';
import type { SimpleWebpage } from './webpage';

const db = new Database('../../data/ranking-diff.sqlite', {
  fileMustExist: false,
});

const setupDB = () => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS queries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      text TEXT NOT NULL,
      UNIQUE(text)
    );
  `);

  db.exec(`
  CREATE INDEX IF NOT EXISTS queries_text_index
    ON queries (text);
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS experiments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS serps (
      queryId INTEGER NOT NULL,
      experimentId INTEGER NOT NULL,
      results TEXT NOT NULL,

      FOREIGN KEY(queryId) REFERENCES
        queries(id),
      FOREIGN KEY(experimentId) REFERENCES
        experiments(id),
      PRIMARY KEY(queryId, experimentId)
    );
  `);

  return db;
};

export const insertQuery = (query: string) => {
  const insertQuery = db.prepare(`
    INSERT OR IGNORE INTO queries (text)
    VALUES (@query)
  `);

  insertQuery.run({ query });
};

export const clearQueries = () => {
  const clearQueries = db.prepare(`
    DELETE FROM queries
  `);

  clearQueries.run();
};

export const deleteQuery = (id: number) => {
  const deleteQuery = db.prepare(`
    DELETE FROM queries
    WHERE id = @id
  `);

  deleteQuery.run({ id });
};

export const getQueries = (): Query[] => {
  const query = db.prepare(`
    SELECT id, text
    FROM queries
  `);

  return query.all() as Query[];
};

export const newExperiment = (name: string): Experiment => {
  const insertExperiment = db.prepare(`
    INSERT OR IGNORE INTO experiments (name)
    VALUES (@name)
  `);

  insertExperiment.run({ name });

  const query = db.prepare(`
    SELECT *
    FROM experiments
    WHERE name = @name
  `);

  return query.get({ name }) as Experiment;
};

export const clearExperiments = () => {
  const clearSerps = db.prepare(`
    DELETE FROM serps
  `);

  clearSerps.run();

  const clearExperiments = db.prepare(`
    DELETE FROM experiments
  `);

  clearExperiments.run();
};

export const deleteExperiment = (id: number) => {
  const deleteSerps = db.prepare(`
    DELETE FROM serps
    WHERE experimentId = @id
  `);

  deleteSerps.run({ id });

  const deleteExperiment = db.prepare(`
    DELETE FROM experiments
    WHERE id = @id
  `);

  deleteExperiment.run({ id });
};

export const renameExperiment = (id: number, name: string) => {
  const renameExperiment = db.prepare(`
    UPDATE experiments
    SET name = @name
    WHERE id = @id
  `);

  renameExperiment.run({ id, name });
};

export const getExperiments = (): Experiment[] => {
  const query = db.prepare(`
    SELECT id, name, timestamp
    FROM experiments
    ORDER BY id DESC
  `);

  return query.all() as Experiment[];
};

export const addSerp = (experimentId: number, queryId: number, webpages: SimpleWebpage[]) => {
  const insertSerp = db.prepare(`
    INSERT OR REPLACE INTO serps (queryId, experimentId, results)

    VALUES (@queryId, @experimentId, @results)
  `);

  insertSerp.run({
    queryId,
    experimentId,
    results: JSON.stringify(webpages),
  });
};

export const queryIntersection = (experimentIdA: number, experimentIdB: number): Query[] => {
  const query = db.prepare(`
    SELECT q.id, q.text
    FROM queries q
    JOIN serps sa ON sa.queryId = q.id
    JOIN serps sb ON sb.queryId = q.id
    WHERE sa.experimentId = @experimentIdA
    AND sb.experimentId = @experimentIdB
  `);

  return query.all({ experimentIdA, experimentIdB }) as Query[];
};

export const experimentById = (id: number): Experiment => {
  const query = db.prepare(`
    SELECT *
    FROM experiments
    WHERE id = @id
  `);

  return query.get({ id }) as Experiment;
};

export const queryById = (id: number): Query => {
  const query = db.prepare(`
    SELECT *
    FROM queries
    WHERE id = @id
  `);

  return query.get({ id }) as Query;
};

export const serpByQueryAndExperiment = (
  queryId: number,
  experimentId: number,
): SimpleWebpage[] => {
  const query = db.prepare(`
    SELECT results
    FROM serps
    WHERE queryId = @queryId
    AND experimentId = @experimentId
  `);

  return JSON.parse(
    (query.get({ queryId, experimentId }) as any).results as string,
  ) as SimpleWebpage[];
};

setupDB();

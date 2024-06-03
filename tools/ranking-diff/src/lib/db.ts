import type { Category, Experiment, LikedState, Query } from '$lib';
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
        queries(id) ON DELETE CASCADE,
      FOREIGN KEY(experimentId) REFERENCES
        experiments(id) ON DELETE CASCADE,
      PRIMARY KEY(queryId, experimentId)
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS liked_experiments (
      queryId INTEGER NOT NULL,
      baselineId INTEGER NOT NULL,
      experimentId INTEGER NOT NULL,
      likedState TEXT NOT NULL,

      FOREIGN KEY(queryId) REFERENCES
        queries(id) ON DELETE CASCADE,
      FOREIGN KEY(baselineId) REFERENCES
        experiments(id) ON DELETE CASCADE,
      FOREIGN KEY(experimentId) REFERENCES
        experiments(id) ON DELETE CASCADE,
      PRIMARY KEY(queryId, baselineId, experimentId)
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      UNIQUE(name)
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS query_category (
      categoryId INTEGER NOT NULL,
      queryId INTEGER NOT NULL,

      FOREIGN KEY(categoryId) REFERENCES
        categories(id) ON DELETE CASCADE,
      FOREIGN KEY(queryId) REFERENCES
        queries(id) ON DELETE CASCADE,
      
      UNIQUE(queryId),

      PRIMARY KEY(categoryId, queryId)
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

export const nextQuery = (experimentId: number, currentQueryId: number): Query | undefined => {
  const query = db.prepare(`
    SELECT q.id, q.text
    FROM queries q
    JOIN serps sa ON sa.queryId = q.id
    WHERE sa.experimentId = @experimentId
    AND q.id > @currentQueryId
    ORDER BY q.id
    LIMIT 1
  `);

  return query.get({ experimentId, currentQueryId }) as Query;
};

export const previousQuery = (experimentId: number, currentQueryId: number): Query | undefined => {
  const query = db.prepare(`
    SELECT q.id, q.text
    FROM queries q
    JOIN serps sa ON sa.queryId = q.id
    WHERE sa.experimentId = @experimentId
    AND q.id < @currentQueryId
    ORDER BY q.id
    LIMIT 1
  `);

  return query.get({ experimentId, currentQueryId }) as Query;
};

export const likedState = (
  baselineId: number,
  experimentId: number,
  queryId: number,
): LikedState => {
  const query = db.prepare(`
    SELECT likedState
    FROM liked_experiments
    WHERE experimentId = @experimentId
    AND queryId = @queryId
    AND baselineId = @baselineId
  `);

  const res = query.get({ baselineId, experimentId, queryId });

  if (res) {
    return (res as any).likedState as LikedState;
  }

  return 'none';
};

export const like = (
  baselineId: number,
  experimentId: number,
  queryId: number,
  state: LikedState,
) =>
  db
    .prepare(
      `
    INSERT OR IGNORE INTO liked_experiments (baselineId, experimentId, queryId, likedState)
    VALUES (@baselineId, @experimentId, @queryId, @state)
  `,
    )
    .run({ baselineId, experimentId, queryId, state });

export const unlike = (baselineId: number, experimentId: number, queryId: number) =>
  db
    .prepare(
      `
    DELETE FROM liked_experiments
    WHERE experimentId = @experimentId
    AND queryId = @queryId
    AND baselineId = @baselineId
  `,
    )
    .run({ baselineId, experimentId, queryId });

export const addCategory = (name: string) => {
  const insertCategory = db.prepare(`
    INSERT OR IGNORE INTO categories (name)
    VALUES (@name)
  `);

  insertCategory.run({ name });
};

export const removeCategory = (categoryId: number) => {
  const deleteCategory = db.prepare(`
    DELETE FROM categories
    WHERE id = @categoryId
  `);

  deleteCategory.run({ categoryId });
};

export const addQueryToCategory = (queryId: number, categoryId: number) => {
  const insertQueryCategory = db.prepare(`
    INSERT OR IGNORE INTO query_category (queryId, categoryId)
    VALUES (@queryId, @categoryId)
  `);

  insertQueryCategory.run({ queryId, categoryId });
};

export const removeQueryCategories = (queryId: number) => {
  const deleteQueryCategory = db.prepare(`
    DELETE FROM query_category
    WHERE queryId = @queryId
  `);

  deleteQueryCategory.run({ queryId });
};

export const getCategories = (): Category[] => {
  const query = db.prepare(`
    SELECT id, name
    FROM categories
  `);

  return query.all() as Category[];
};

export const getQueriesByCategory = (categoryId: number) => {
  const query = db.prepare(`
    SELECT q.id, q.text
    FROM queries q
    JOIN query_category qc ON qc.queryId = q.id
    WHERE categoryId = @categoryId
  `);

  return query.all({ categoryId });
};

export const getQueryCategories = (queryId: number): Category[] => {
  const query = db.prepare(`
    SELECT c.id, c.name
    FROM categories c
    JOIN query_category qc ON qc.categoryId = c.id
    WHERE queryId = @queryId
  `);

  return query.all({ queryId }) as Category[];
};

export const clearCategories = () => {
  const clearCategories = db.prepare(`
    DELETE FROM categories
  `);

  clearCategories.run();
};

setupDB();

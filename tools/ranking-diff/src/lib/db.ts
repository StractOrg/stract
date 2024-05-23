import type { Experiment, Query } from '$lib';
import Database from 'better-sqlite3';

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
      name TEXT NOT NULL
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

export const newExperiment = (name: string) => {
  const insertExperiment = db.prepare(`
    INSERT OR IGNORE INTO experiments (name)
    VALUES (@name)
  `);

  insertExperiment.run({ name });
};

export const clearExperiments = () => {
  const clearExperiments = db.prepare(`
    DELETE FROM experiments
  `);

  clearExperiments.run();
};

export const deleteExperiment = (id: number) => {
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
    SELECT id, name
    FROM experiments
    ORDER BY id DESC
  `);

  return query.all() as Experiment[];
};

setupDB();

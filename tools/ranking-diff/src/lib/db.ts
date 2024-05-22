import type { Experiment, Query } from '$lib';
import Database from 'better-sqlite3';

const db = new Database('../../data/ranking-diff.sqlite', {
  fileMustExist: false,
});

const setupDB = () => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS queries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      text TEXT NOT NULL
    );
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
    INSERT OR IGNORE INTO queries (query)
    VALUES (@query)
  `);

  insertQuery.run({ query });
};

export const getQueries = (): Query[] => {
  const query = db.prepare(`
    SELECT id, query
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

export const getExperiments = (): Experiment[] => {
  const query = db.prepare(`
    SELECT id, name
    FROM experiments
  `);

  return query.all() as Experiment[];
};

setupDB();

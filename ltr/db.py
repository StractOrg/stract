import sqlite3
import json

class Db:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.setup_db()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def setup_db(self):
      self.cursor.execute(
          """
          CREATE TABLE IF NOT EXISTS queries (
              qid UUID PRIMARY KEY DEFAULT (HEX(RANDOMBLOB(16))),
              query TEXT NOT NULL UNIQUE
          );"""
      )

      self.cursor.execute(
          """
          CREATE TABLE IF NOT EXISTS search_results (
              qid UUID NOT NULL,
              url TEXT NOT NULL,
              orig_rank INTEGER NOT NULL,
              webpage_json TEXT NOT NULL,
              annotation INTEGER DEFAULT NULL,
              PRIMARY KEY (qid, url)
          );"""
      )

      self.conn.commit()

    def add_query(self, query: str):
      self.cursor.execute(
          """
          INSERT OR IGNORE INTO queries (query)
          VALUES (?)
          """,
          (query,),
      )
      self.conn.commit()

    def get_unannotated_queries(self):
      unannotated_queries = self.cursor.execute(
          """
          SELECT qid, query
          FROM queries
          WHERE NOT EXISTS (
              SELECT 1 FROM search_results WHERE search_results.qid = queries.qid AND search_results.annotation IS NOT NULL
          )
          ORDER BY qid
          """,
      ).fetchall()

      return {qid: query for qid, query in unannotated_queries}

    def insert_result(self, qid, rank, result):
      self.cursor.execute(
          """
          INSERT OR IGNORE INTO search_results (qid, url, orig_rank, webpage_json)
          VALUES (?, ?, ?, ?)
          """,
          (qid, result["url"], rank, json.dumps(result)),
      )

      self.conn.commit()

    def insert_results(self, qid, results):
      for i, result in enumerate(results):
        self.insert_result(qid, i, result)


    def get_unannotated_results(self, qid):
      return self.cursor.execute(
          """
          SELECT url, orig_rank, webpage_json
          FROM search_results
          WHERE qid = ? AND annotation IS NULL
          ORDER BY orig_rank
          """,
          (qid,),
      ).fetchall()

    def annotate(self, qid, url, annotation):
      self.cursor.execute(
          """
          UPDATE search_results
          SET annotation = ?
          WHERE qid = ? AND url = ?
          """,
          (annotation, qid, url),
      )
      self.conn.commit()

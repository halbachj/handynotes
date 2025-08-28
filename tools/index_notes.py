#!/usr/bin/env python3
import sqlite3, os, glob, yaml
from pypdf import PdfReader

DB = "index.sqlite"
PDF_DIR = "data/scans_ocr"
META_DIR = "meta"

schema = """
CREATE TABLE IF NOT EXISTS docs(
  id INTEGER PRIMARY KEY,
  path TEXT UNIQUE,
  title TEXT,
  course TEXT,
  term TEXT,
  lecture INTEGER,
  tags TEXT,
  mtime REAL,
  text TEXT
);
CREATE INDEX IF NOT EXISTS idx_docs_text ON docs(text);
CREATE INDEX IF NOT EXISTS idx_docs_tags ON docs(tags);
"""

def read_yaml_sidecar(basename):
  ypath = os.path.join(META_DIR, f"{basename}.yml")
  if not os.path.exists(ypath): return {}
  with open(ypath, "r") as f: return yaml.safe_load(f) or {}

def extract_text(pdf_path, max_pages=100):
  try:
    r = PdfReader(pdf_path)
    pages = r.pages[:max_pages]
    return "\n".join([p.extract_text() or "" for p in pages])
  except Exception as e:
    return f"[EXTRACT_FAIL] {e}"

def upsert(cur, path, meta, mtime, text):
  cur.execute("""INSERT INTO docs(path,title,course,term,lecture,tags,mtime,text)
                 VALUES(?,?,?,?,?,?,?,?)
                 ON CONFLICT(path) DO UPDATE SET
                   title=excluded.title, course=excluded.course, term=excluded.term,
                   lecture=excluded.lecture, tags=excluded.tags,
                   mtime=excluded.mtime, text=excluded.text""",
              (path,
               meta.get("title"),
               meta.get("course"),
               meta.get("term"),
               meta.get("lecture"),
               ",".join(meta.get("tags", [])) if meta.get("tags") else None,
               mtime, text))

def main():
  con = sqlite3.connect(DB)
  cur = con.cursor()
  cur.executescript(schema)
  for pdf in glob.glob(os.path.join(PDF_DIR, "*.pdf")):
    base = os.path.splitext(os.path.basename(pdf))[0]
    mtime = os.path.getmtime(pdf)
    cur.execute("SELECT mtime FROM docs WHERE path=?", (pdf,))
    row = cur.fetchone()
    if row and row[0] == mtime:
      continue
    meta = read_yaml_sidecar(base)
    text = extract_text(pdf)
    upsert(cur, pdf, meta, mtime, text)
  con.commit(); con.close()
  print("Index updated.")

if __name__ == "__main__":
  main()


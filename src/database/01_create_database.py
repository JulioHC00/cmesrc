from src.cmesrc.config import CMESRC_DB
print(CMESRC_DB)

import sqlite3
from os.path import exists

if exists(CMESRC_DB):
    raise Exception("Database already exists")

con = sqlite3.connect(CMESRC_DB)
con.execute("PRAGMA foreign_keys = ON")
cur = con.cursor()

cur.executescript("""
CREATE TABLE harps (
  harpnum INTEGER PRIMARY KEY,
  start TEXT NOT NULL REFERENCES images (timestamp),
  end TEXT NOT NULL REFERENCES images (timestamp)
);

CREATE TABLE harps_bbox (
  harpnum INTEGER REFERENCES harps (harpnum),
  timestamp TEXT REFERENCES images (timestamp),
  LONDTMIN REAL,
  LONDTMAX REAL,
  LATDTMIN REAL,
  LATDTMAX REAL,
  IRBB INTEGER,
  PRIMARY KEY (harpnum, timestamp)
);

CREATE TABLE cmes (
  cme_id INTEGER PRIMARY KEY,
  timestamp TEXT NOT NULL REFERENCES images (timestamp),
  harps_sr INTEGER REFERENCES harps (harpnum)
);

CREATE TABLE flares (
  flare_id INTEGER PRIMARY KEY,
  harpnum INTEGER REFERENCES harps (harpnum),
  cme INTEGER REFERENCES cmes (cme_id),
  timestamp TEXT NOT NULL REFERENCES images (timestamp),
  class TEXT NOT NULL,
  class_score REAL NOT NULL,
  LON REAL NOT NULL,
  LAT REAL NOT NULL,
  verification TEXT NOT NULL
);

CREATE TABLE dimmings (
  dimming_id INTEGER PRIMARY KEY,
  harpnum INTEGER REFERENCES harps (harpnum),
  cme INTEGER REFERENCES cmes (cme_id),
  timestamp TEXT NOT NULL REFERENCES images (timestamp),
  LON REAL NOT NULL,
  LAT REAL NOT NULL
);

CREATE TABLE harps_pixel_bbox (
  harpnum INTEGER REFERENCES harps (harpnum),
  timestamp TEXT REFERENCES images (timestamp),
  x_min INTEGER NOT NULL REFERENCES x_pixel_values (x),
  x_max INTEGER NOT NULL REFERENCES x_pixel_values (x),
  y_min INTEGER NOT NULL REFERENCES y_pixel_values (y),
  y_max INTEGER NOT NULL REFERENCES y_pixel_values (y),
  x_cen INTEGER NOT NULL REFERENCES x_pixel_values (x),
  y_cen INTEGER NOT NULL REFERENCES y_pixel_values (y),
  PRIMARY KEY (harpnum, timestamp),
  FOREIGN KEY (harpnum, timestamp) REFERENCES harps_bbox (harpnum, timestamp)
);

CREATE TABLE images (
  timestamp TEXT NOT NULL UNIQUE,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL,
  hour INTEGER NOT NULL,
  minute INTEGER NOT NULL,
  second INTEGER NOT NULL,
  idx INTEGER NOT NULL
);

CREATE TABLE pixel_values (
  value_id INTEGER PRIMARY KEY,
  description TEXT NOT NULL UNIQUE
);

CREATE TABLE pixels (
  timestamp TEXT REFERENCES images (timestamp),
  x INTEGER REFERENCES x_pixel_values (x),
  y INTEGER REFERENCES y_pixel_values (y),
  value INTEGER REFERENCES pixel_values (value_id),
  PRIMARY KEY (timestamp, x, y)
);

CREATE TABLE x_pixel_values (
  x INTEGER NOT NULL PRIMARY KEY
);

CREATE TABLE y_pixel_values (
  y INTEGER NOT NULL PRIMARY KEY
);
""")

cur.close()
con.close()
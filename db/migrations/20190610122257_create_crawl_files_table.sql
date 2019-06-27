-- migrate:up

-- Create a table to store information about the WARC (and log?) files
--- CockroachDB does not support ENUM yet, so commented out:
--- CREATE TYPE file_type AS ENUM ('warc', 'invalid-warc', 'viral', 'crawl-log','log', 'dlx', 'cdx', 'unknown');
--- CREATE TYPE crawl_stream AS ENUM ('selective', 'domain', 'frequent');
--- CREATE TYPE crawl_terms AS ENUM ('npld', 'by-permission')
CREATE TABLE crawl_files (
  filename TEXT,                   -- The filename
  job_name TEXT,                   -- The crawl job name
  job_launch TIMESTAMP,            -- The timestamp of the job launch for this file.
  full_path TEXT,                  -- Full path of file on HDFS
  extension TEXT,                  -- The file extension (to help understand files uncategorized by type)
  type TEXT, --- file_type,        -- e.g. 'warc'
  size INT,                        -- The size of the file in bytes.
  created_at TIMESTAMP,            -- The timestamp of the creation of this WARC
  last_seen_at TIMESTAMP,          -- The timestamp we last saw this WARC
  last_hashed_at TIMESTAMP,        -- The timestamp we last checked the checksum of this WARC
  storage_uri TEXT ARRAY,          -- List of URIs indicating where this file is stored
  stream TEXT, --- crawl_stream,   -- The stream this WARC was collected under (see above)
  terms TEXT, --- crawl_terms,     -- The terms this WARC was collected under (see above)
  pids TEXT ARRAY,                 -- Permanent IDentifiers for this resource (e.g. an ARK)
  cdx_index_status TEXT,           -- The CDX index status, indicating this WARC is accessible via Wayback
  solr_index_status TEXT,          -- The Solr index status, indicating this WARC is available for full text search.
  digest TEXT,                     -- The hash for this file (for integrity checking)
  stats JSONB,                     -- A JSON column to hold arbitrary stats for this WARC.
  PRIMARY KEY (filename, job_name, job_launch)
);
-- Create indexes for fast lookups and time filtering:
CREATE INDEX ON crawl_files (filename);
CREATE INDEX ON crawl_files (digest);
CREATE INDEX ON crawl_files (full_path);
CREATE INDEX ON crawl_files (created_at);
CREATE INDEX ON crawl_files (ppid);
CREATE INDEX ON crawl_files (cdx_index_status);
CREATE INDEX ON crawl_files (solr_index_status);
CREATE INDEX ON crawl_files GIN (stats); -- Use an inverted index for JSONB of stats


-- migrate:down

drop table crawl_files;

# Explore: EU EP crawl prototype

This folder contains a standalone, non-indexing prototype to fetch a few European Parliament documents and save them locally for inspection.

- Types: `cre` (XML), `a` (PDF), `ta` (PDF)
- Language: English (`_EN`)
- Output: `explore/out/eu/{cre|a|ta}/...`

Run it:
- Python 3.10+
- Dependencies: only `requests` (already used in the project). PDF text extraction is not required because we save raw files.

To execute:
- Open the file `explore/eu_ep_crawl_prototype.py` and run it. It crawls a few recent documents and writes them into the `out` folder.

Notes:
- The prototype is intentionally slow (adds ~1s between requests) and stops sequences on first 404.
- CRE are saved as XML, A/TA as PDFs.

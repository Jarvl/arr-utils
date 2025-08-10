# arr-utils
A collection of utilities to manage arr applications

## grab_purger

Purges downloaded episodes in Sonarr whose most recent grab came from a specific indexer (by checking the grab's Info URL for a substring), deletes the file and triggers an EpisodeSearch.

Install dependencies, set env vars, and run via Poetry:

```bash
$ poetry install
$ cp grab_purger/.env.example grab_purger/.env
$ poetry run grab-purger
```

Dry run mode can be done by setting `DRY_RUN=true` in `.env`


### pyenv

This repo pins Python in `.python-version` (e.g., `3.12.5`). To use pyenv:

- Install the version: `pyenv install --skip-existing 3.12.5`
- Set local version (if needed): `pyenv local 3.12.5`
- Ensure shims are active: `pyenv rehash`
- Then install and run with Poetry as above.

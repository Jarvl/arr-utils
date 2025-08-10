import os
import sys
import re
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

import httpx
from dotenv import load_dotenv, find_dotenv


@dataclass
class Config:
    sonarr_url: str
    api_key: str
    indexer_substring: str
    page_size: int = 250
    dry_run: bool = False
    series_concurrency: int = 25
    episode_concurrency: int = 50
    verbose: bool = False
    request_timeout: float = 60.0
    episode_timeout: float = 120.0
    progress_every: int = 100
    history_page_size: int = 1000
    history_max_pages: int = 100


def load_config_from_env() -> Config:
    base_url = os.environ.get("SONARR_URL", "http://localhost:8989")
    api_key = os.environ.get("SONARR_API_KEY")
    if not api_key:
        print("Missing SONARR_API_KEY env var", file=sys.stderr)
        sys.exit(2)

    indexer_sub = os.environ.get("GRAB_PURGER_INDEXER_SUBSTRING", "torrentgalaxy.one").strip()

    dry = os.environ.get("DRY_RUN", "false").lower() in {"1", "true", "yes", "y"}
    page_size_str = os.environ.get("SONARR_PAGE_SIZE", "250")
    try:
        page_size = max(1, min(1000, int(page_size_str)))
    except ValueError:
        page_size = 250

    try:
        series_cc = max(1, int(os.environ.get("SERIES_CONCURRENCY", "4")))
    except ValueError:
        series_cc = 4
    try:
        episode_cc = max(1, int(os.environ.get("EPISODE_CONCURRENCY", "10")))
    except ValueError:
        episode_cc = 10
    verbose = os.environ.get("GRAB_PURGER_VERBOSE", os.environ.get("VERBOSE", "false")).lower() in {"1", "true", "yes", "y"}

    # Optional timeouts and progress
    def _float_env(name: str, default: float) -> float:
        try:
            return float(os.environ.get(name, str(default)))
        except ValueError:
            return default

    request_timeout = _float_env("REQUEST_TIMEOUT", 60.0)
    episode_timeout = _float_env("EPISODE_TIMEOUT", 120.0)
    try:
        progress_every = max(1, int(os.environ.get("PROGRESS_EVERY", "25")))
    except ValueError:
        progress_every = 25
    try:
        history_page_size = max(10, int(os.environ.get("HISTORY_PAGE_SIZE", "1000")))
    except ValueError:
        history_page_size = 1000
    try:
        history_max_pages = max(1, int(os.environ.get("HISTORY_MAX_PAGES", "100")))
    except ValueError:
        history_max_pages = 100

    return Config(
        sonarr_url=base_url.rstrip("/"),
        api_key=api_key,
        indexer_substring=indexer_sub,
        page_size=page_size,
        dry_run=dry,
        series_concurrency=series_cc,
        episode_concurrency=episode_cc,
        verbose=verbose,
        request_timeout=request_timeout,
        episode_timeout=episode_timeout,
        progress_every=progress_every,
        history_page_size=history_page_size,
        history_max_pages=history_max_pages,
    )


class SonarrClient:
    def __init__(self, base_url: str, api_key: str, client: Optional[httpx.AsyncClient] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client or httpx.AsyncClient(headers={"X-Api-Key": api_key}, timeout=60)
        # ensure header present if external client passed
        self.client.headers.update({"X-Api-Key": api_key})

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}/api/v3{path}"

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = await self.client.get(self._url(path), params=params)
        resp.raise_for_status()
        return resp.json()

    async def post(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        resp = await self.client.post(self._url(path), json=payload)
        resp.raise_for_status()
        if resp.text:
            try:
                return resp.json()
            except Exception:
                return resp.text
        return None

    async def delete(self, path: str) -> None:
        resp = await self.client.delete(self._url(path))
        resp.raise_for_status()

    # High-level helpers
    async def get_all_series(self) -> List[Dict[str, Any]]:
        return await self.get("/series")

    async def get_episodes_for_series(self, series_id: int) -> List[Dict[str, Any]]:
        return await self.get("/episode", params={"seriesId": series_id})

    async def get_episode_files_for_series(self, series_id: int) -> List[Dict[str, Any]]:
        return await self.get("/episodefile", params={"seriesId": series_id})

    async def get_history_for_episode(self, episode_id: int, page_size: int = 250) -> List[Dict[str, Any]]:
        # Sonarr history is paged via paging model: /history?episodeId=xx&page=1&pageSize=250
        page = 1
        records: List[Dict[str, Any]] = []
        while True:
            data = await self.get(
                "/history",
                params={"episodeId": episode_id, "page": page, "pageSize": page_size},
            )
            page_records = data.get("records", []) if isinstance(data, dict) else []
            if not page_records:
                break
            records.extend(page_records)
            total_records = data.get("totalRecords", len(records))
            if len(records) >= total_records:
                break
            page += 1
        return records

    async def get_history_for_series(self, series_id: int, page_size: int = 1000, max_pages: int = 100) -> List[Dict[str, Any]]:
        # Prefer series-scoped history to avoid per-episode calls
        page = 1
        records: List[Dict[str, Any]] = []
        print(f"History fetch start: seriesId={series_id} pageSize={page_size} maxPages={max_pages}")
        while True:
            data = await self.get(
                "/history",
                params={"seriesId": series_id, "page": page, "pageSize": page_size},
            )
            page_records = data.get("records", []) if isinstance(data, dict) else []
            if not page_records:
                print(f"History page {page}: 0 records -> done")
                break
            records.extend(page_records)
            print(f"History page {page}: +{len(page_records)} records (total={len(records)})")
            total_records = data.get("totalRecords", len(records))
            if len(records) >= total_records:
                print(f"History fetched all records: totalRecords={total_records}")
                break
            if page >= max_pages:
                print(f"History reached max pages ({max_pages}); continuing with partial history")
                break
            page += 1
        print(f"History fetch end: seriesId={series_id} totalFetched={len(records)} pages={page}")
        return records

    async def get_latest_grab_for_episode(self, episode_id: int) -> Optional[Dict[str, Any]]:
        # Try to retrieve only the latest grabbed entry using paging and eventType filter
        try:
            data = await self.get(
                "/history",
                params={
                    "episodeId": episode_id,
                    "eventType": 1,  # grabbed
                    "page": 1,
                    "pageSize": 1,
                },
            )
            records = data.get("records", []) if isinstance(data, dict) else []
            if records:
                return records[0]
        except Exception:
            # Fallthrough to full episode history below
            pass
        # Fallback: fetch a small page and pick most recent grab client-side
        history = await self.get_history_for_episode(episode_id, page_size=50)
        grabs = [rec for rec in history if _is_grab_event(rec)]
        if not grabs:
            return None
        grabs.sort(key=_event_date, reverse=True)
        return grabs[0]

    async def delete_episode_file(self, episode_file_id: int) -> None:
        await self.delete(f"/episodefile/{episode_file_id}")

    async def trigger_episode_search(self, episode_ids: List[int]) -> Dict[str, Any]:
        return await self.post("/command", {"name": "EpisodeSearch", "episodeIds": episode_ids})


def _is_grab_event(rec: Dict[str, Any]) -> bool:
    et = rec.get("eventType")
    return et == 1 or et == "grabbed" or et == "grab"


def _is_import_event(rec: Dict[str, Any]) -> bool:
    et = rec.get("eventType")
    if et in (3, "downloadFolderImported", "episodeFileImported", "imported"):
        return True
    data = rec.get("data") or {}
    # Heuristic: imports usually carry downloadId and releaseTitle
    return ("downloadId" in data and "releaseTitle" in data) and not _is_grab_event(rec)


def _event_date(rec: Dict[str, Any]) -> str:
    return rec.get("date") or rec.get("created", "")


def find_grab_for_current_file(history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find the grab corresponding to the most recent import for the episode.

    This better reflects the grab for the file currently on disk.
    """
    if not history:
        return None
    # newest import
    imports = [rec for rec in history if _is_import_event(rec)]
    if not imports:
        # fall back to most recent grab overall
        grabs = [rec for rec in history if _is_grab_event(rec)]
        if not grabs:
            return None
        grabs.sort(key=_event_date, reverse=True)
        return grabs[0]

    imports.sort(key=_event_date, reverse=True)
    latest_import = imports[0]
    import_time = _event_date(latest_import)
    import_dl_id = (latest_import.get("data") or {}).get("downloadId")

    # Prefer a grab with the same downloadId
    candidate_grabs = [rec for rec in history if _is_grab_event(rec)]
    if import_dl_id:
        for rec in sorted(candidate_grabs, key=_event_date, reverse=True):
            dl_id = (rec.get("data") or {}).get("downloadId")
            if dl_id and dl_id == import_dl_id:
                return rec

    # Otherwise, find the last grab before the import
    candidate_grabs.sort(key=_event_date, reverse=True)
    for rec in candidate_grabs:
        if _event_date(rec) <= import_time:
            return rec
    # As a last resort, return newest grab
    return candidate_grabs[0] if candidate_grabs else None


def extract_release_title(history_item: Dict[str, Any]) -> str:
    data = history_item.get("data") or {}
    title = data.get("releaseTitle") or data.get("release") or ""
    return str(title)


def extract_indexer_fields(history_item: Dict[str, Any]) -> Dict[str, str]:
    data = history_item.get("data") or {}
    candidates: Dict[str, str] = {}
    # Indexer name (not a URL) but useful as a fallback
    if data.get("indexer"):
        candidates["indexer"] = str(data.get("indexer"))
    return candidates


def should_purge_by_indexer(fields: Dict[str, str], indexer_substring: str) -> bool:
    if not fields or not indexer_substring:
        return False
    needle = indexer_substring.lower()
    for key, value in fields.items():
        try:
            if needle in value.lower():
                return True
        except Exception:
            continue
    return False


def build_episode_file_index(episode_files: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    # Map episodeId -> file
    index: Dict[int, Dict[str, Any]] = {}
    for ef in episode_files:
        # In Sonarr v3, EpisodeFile contains 'episodeIds' for multi-episode files
        ids = ef.get("episodeIds")
        if isinstance(ids, list) and ids:
            for single_id in ids:
                index[int(single_id)] = ef
            continue
        ep_id = ef.get("episodeId")
        if isinstance(ep_id, list):
            for single_id in ep_id:
                index[int(single_id)] = ef
        elif ep_id is not None:
            index[int(ep_id)] = ef
    return index


async def run_async(config: Config) -> int:
    total_purged = 0
    total_lock = asyncio.Lock()

    limits = httpx.Limits(max_connections=max(10, config.series_concurrency * 2), max_keepalive_connections=20)
    async with httpx.AsyncClient(headers={"X-Api-Key": config.api_key}, timeout=config.request_timeout, limits=limits, http2=True) as http_client:
        client = SonarrClient(config.sonarr_url, config.api_key, http_client)

        series_list = await client.get_all_series()

        series_sema = asyncio.Semaphore(config.series_concurrency)
        episode_sema = asyncio.Semaphore(config.episode_concurrency)
        processed_count = 0
        processed_lock = asyncio.Lock()

        async def process_episode(series: Dict[str, Any], ep: Dict[str, Any], grab: Dict[str, Any]) -> None:
            nonlocal total_purged
            nonlocal processed_count
            if not ep.get("hasFile"):
                return
            episode_id = ep["id"]
            episode_file_id = ep.get("episodeFileId")
            if not episode_file_id:
                # Final fallback: cannot act without a file id
                if config.verbose:
                    print(
                        f"Skip(no-episodeFileId): series='{series.get('title')}' S{ep.get('seasonNumber')}E{ep.get('episodeNumber')} episodeId={episode_id}"
                    )
                return
            async with episode_sema:
                try:
                    await asyncio.wait_for(
                        _process_episode_inner(series, ep, episode_id, episode_file_id, grab),
                        timeout=config.episode_timeout,
                    )
                except asyncio.TimeoutError:
                    print(f"Timeout processing episodeId={episode_id}", file=sys.stderr)
                except Exception as e:
                    print(f"Error processing episodeId={episode_id}: {e}", file=sys.stderr)
                finally:
                    async with processed_lock:
                        processed_count += 1
                        if processed_count % config.progress_every == 0:
                            print(f"Progress: processed {processed_count} episodes, purged {total_purged}")

        deleted_file_ids: set[int] = set()
        deleted_file_ids_lock = asyncio.Lock()

        search_episode_ids: List[int] = []
        search_ids_lock = asyncio.Lock()

        async def _process_episode_inner(series: Dict[str, Any], ep: Dict[str, Any], episode_id: int, episode_file_id: int, grab: Dict[str, Any]) -> None:
            nonlocal total_purged
            release_title = extract_release_title(grab)
            indexer_fields = extract_indexer_fields(grab)
            match = should_purge_by_indexer(indexer_fields, config.indexer_substring)
            if config.verbose:
                print(
                    f"Candidate match={match}: series='{series.get('title')}' S{ep.get('seasonNumber')}E{ep.get('episodeNumber')} "
                    f"episodeId={episode_id} fields={indexer_fields} title='{release_title}'"
                )
            if not match:
                return

            history_id = grab.get("id")

            print(
                f"Match: series='{series.get('title')}' S{ep.get('seasonNumber')}E{ep.get('episodeNumber')} "
                f"episodeId={episode_id} fileId={episode_file_id} title='{release_title}' fields={indexer_fields}"
            )

            if config.dry_run:
                print("Dry run, skipping delete")
            else:
                # Delete episode file
                async with deleted_file_ids_lock:
                    already_deleted = int(episode_file_id) in deleted_file_ids
                if not already_deleted:
                    try:
                        await client.delete_episode_file(int(episode_file_id))
                        print(f"Deleted file id={episode_file_id}")
                        async with deleted_file_ids_lock:
                            deleted_file_ids.add(int(episode_file_id))
                    except Exception as e:  # noqa: BLE001
                        print(f"Failed deleting file id={episode_file_id}: {e}", file=sys.stderr)
                        return


                # Queue for batch search
                async with search_ids_lock:
                    search_episode_ids.append(episode_id)

            async with total_lock:
                total_purged += 1

        async def process_series(series: Dict[str, Any]) -> None:
            async with series_sema:
                series_id = series["id"]
                episodes = await client.get_episodes_for_series(series_id)
                # Filter only episodes that have files and ids
                episodes_with_files = [ep for ep in episodes if ep.get("hasFile") and ep.get("episodeFileId")]

                # Launch episode tasks: fetch only the most recent grab per episode
                tasks: List[asyncio.Task] = []
                for ep in episodes_with_files:
                    ep_id = int(ep["id"])
                    async def launch(ep_local: Dict[str, Any], eid: int) -> None:
                        try:
                            grab = await client.get_latest_grab_for_episode(eid)
                        except Exception as e:
                            print(f"Error fetching latest grab for episodeId={eid}: {e}", file=sys.stderr)
                            return
                        if not grab:
                            if config.verbose:
                                print(
                                    f"Skip(no-latest-grab): series='{series.get('title')}' S{ep_local.get('seasonNumber')}E{ep_local.get('episodeNumber')} episodeId={eid}"
                                )
                            return
                        await process_episode(series, ep_local, grab)
                    tasks.append(asyncio.create_task(launch(ep, ep_id)))

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                # Batch EpisodeSearch for processed episodes
                if not config.dry_run:
                    async with search_ids_lock:
                        ids = list(dict.fromkeys(search_episode_ids))
                        search_episode_ids.clear()
                    if ids:
                        # Chunk size to keep payloads reasonable
                        chunk_size = 100
                        for start in range(0, len(ids), chunk_size):
                            chunk = ids[start:start + chunk_size]
                            try:
                                await client.trigger_episode_search(chunk)
                                print(f"Triggered search for {len(chunk)} episodes in seriesId={series_id}")
                            except Exception as e:  # noqa: BLE001
                                print(f"Failed batch search for seriesId={series_id}: {e}", file=sys.stderr)


        results = await asyncio.gather(*(process_series(s) for s in series_list), return_exceptions=True)
        failures = sum(1 for r in results if isinstance(r, Exception))
        if failures:
            print(f"Completed with {failures} series failures", file=sys.stderr)

    print(f"Done. Purged {total_purged} episodes.")
    return 0


def main() -> None:
    # Load environment variables from .env if present
    # 1) Load from current working directory if present
    load_dotenv(find_dotenv(usecwd=True))
    # 2) Also try loading from the package directory (grab_purger/.env)
    pkg_dir = os.path.dirname(__file__)
    pkg_env = os.path.join(pkg_dir, ".env")
    if os.path.exists(pkg_env):
        load_dotenv(pkg_env, override=False)

    config = load_config_from_env()
    try:
        code = asyncio.run(run_async(config))
    except httpx.HTTPError as http_err:
        print(f"HTTP error: {http_err}", file=sys.stderr)
        try:
            # httpx errors may have response
            if getattr(http_err, "response", None) is not None:
                print(
                    f"Response: {http_err.response.status_code} {http_err.response.text}",
                    file=sys.stderr,
                )
        except Exception:
            pass
        sys.exit(1)
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    sys.exit(code)


if __name__ == "__main__":
    main()



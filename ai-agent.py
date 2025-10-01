import os, time, json, random, logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


class SpotifyAgent:
    # Region → name variants we'll try in search (order matters)
    REGION_NAME_VARIANTS = {
        "US": ["USA", "United States", "US"],
        "GB": ["UK", "United Kingdom", "Great Britain", "GB"],
        "FR": ["France", "FR", "Fr", "French"],
        "JP": ["Japan", "JP"],
        "BR": ["Brazil", "Brasil", "BR"],
        "CA": ["Canada", "CA"],
        "DE": ["Germany", "DE"],
        "ES": ["Spain", "ES"],
        "IT": ["Italy", "IT"],
        "AU": ["Australia", "AU"],
    }

    # Owners we prefer to trust (first match wins)
    PREFERRED_OWNERS = {"spotify", "spotifycharts", "topsify"}

    def __init__(self, config_file: str):
        self.config = self._load_config(config_file)

        # make sure log dir exists BEFORE FileHandler
        Path("logs").mkdir(parents=True, exist_ok=True)
        self._setup_logging()
        self._setup_auth()

        self.delay_multiplier = 1.0
        self.data = []  # list of dict rows
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "total_requests": 0,
            "success": 0,
            "fail": 0,
            "apis_used": ["spotify"],
            "quality_scores": [],
        }

        self.output_dir = Path(self.config.get("output_dir", "data"))
        (self.output_dir / "raw").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "processed").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "metadata").mkdir(parents=True, exist_ok=True)

    # ---------- setup ----------
    def _load_config(self, f):
        with open(f) as fh:
            return json.load(fh)

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            handlers=[logging.FileHandler("logs/collection.log"), logging.StreamHandler()],
        )
        self.logger = logging.getLogger("SpotifyAgent")

    def _setup_auth(self):
        load_dotenv()
        # support either naming scheme you’ve been using
        cid = os.getenv("CLIENT_ID")
        secret = os.getenv("CLIENT_SECRET")
        if not cid or not secret:
            raise RuntimeError(
                "Missing CLIENT_ID/CLIENT_SECRET (or SPOTIFY_CLIENT_ID/SECRET) in .env"
            )
        auth = SpotifyClientCredentials(client_id=cid, client_secret=secret)
        self.sp = spotipy.Spotify(auth_manager=auth, requests_timeout=20, retries=2)

    # ---------- main flow ----------
    def run(self):
        self.logger.info("Starting collection")
        try:
            for region in self.config["regions"]:
                for template in self.config["playlists"]:
                    # Resolve a playlist id by searching region-aware variants
                    pid, label = self._resolve_playlist(region, template)
                    if not pid:
                        self.logger.warning(f"Playlist not found for region {region}")
                        self.stats["fail"] += 1
                        continue

                    # Collect with explicit market to reduce regional issues
                    self._collect_playlist_tracks(pid, label, market=region)
                    self.stats["success"] += 1
                    self._respectful_delay()

            self._write_outputs()
            self._write_metadata_and_quality()
        except Exception as e:
            self.logger.exception(f"Fatal error: {e}")
        finally:
            self.logger.info("Done.")

    # ---------- playlist resolution (search-first, region-aware) ----------
    def _resolve_playlist(self, region_code: str, template: str):
        """
        Try a few candidate names for the region and return (playlist_id, label).
        """
        # Build candidate display names
        variants = self.REGION_NAME_VARIANTS.get(region_code, [region_code])
        candidates = []
        for v in variants:
            candidates += [
                f"Top 50 - {v}",
                f"Top 50 {v}",
                f"{v} Top 50",
            ]

        # Final fallback: use the raw template substitution
        candidates.append(template.replace("%REGION%", region_code))

        # Search each candidate, preferring official owners
        for name in candidates:
            pid = self._search_playlist_id(name, market=region_code)
            if pid:
                self.logger.info(f"Resolved '{name}' → {pid}")
                return pid, name

        # nothing found
        return None, None

    def _search_playlist_id(self, playlist_name: str, market: str = None):
        """
        Search for a playlist by name; prefer known owners if present.
        """
        self.stats["total_requests"] += 1
        try:
            res = self.sp.search(
                q=f'playlist:"{playlist_name}"', type="playlist", limit=10, market=market
            )
            playlists_obj = (res or {}).get("playlists") or {}
            items = playlists_obj.get("items") or []
            if not items:
                return None

            # Try to pick a trusted owner first
            for it in items:
                owner = ((it or {}).get("owner") or {}).get("id", "")
                pid = (it or {}).get("id")
                if pid and owner and owner.lower() in self.PREFERRED_OWNERS:
                    return pid

            # else just take the first valid result
            first = items[0] or {}
            return first.get("id")
        except Exception as e:
            self.logger.warning(f"Search failed for {playlist_name}: {e}")
            return None

    # ---------- collection ----------
    def _collect_playlist_tracks(self, playlist_id: str, playlist_label: str, market: str = None):
        limit = self.config.get("max_tracks_per_playlist", 50)
        self.logger.info(f"Fetching tracks for {playlist_label}")
        offset, grabbed = 0, 0
        while grabbed < limit:
            self.stats["total_requests"] += 1
            page = self.sp.playlist_items(
                playlist_id,
                limit=min(50, limit - grabbed),
                offset=offset,
                market=market,  # helps with regional availability
            )
            items = (page or {}).get("items") or []
            if not items:
                break
            for it in items:
                tr = (it or {}).get("track") or {}
                if not tr:
                    continue
                row = {
                    "playlist": playlist_label,
                    "track_id": tr.get("id"),
                    "track_name": tr.get("name"),
                    "artist_name": ", ".join(a.get("name", "") for a in tr.get("artists", [])),
                    "album_name": (tr.get("album") or {}).get("name"),
                    "release_date": (tr.get("album") or {}).get("release_date"),
                    "popularity": tr.get("popularity"),
                    "added_at": it.get("added_at"),
                }
                self.data.append(row)
            grabbed += len(items)
            offset += len(items)
            if len(items) == 0:
                break
            self._respectful_delay()

    # ---------- quality / adaptation ----------
    def _respectful_delay(self):
        base = self.config.get("base_delay", 1.0) * self.delay_multiplier
        time.sleep(base * random.uniform(0.6, 1.6))

    def _adapt_on_failure(self):
        self.delay_multiplier = min(self.delay_multiplier * 1.5, 8.0)

    def _quality_checks(self) -> dict:
        df = pd.DataFrame(self.data)
        if df.empty:
            return {"completeness": 0, "duplicates": 0, "consistency": 0, "score": 0}

        critical = ["track_id", "track_name", "artist_name", "popularity"]
        existing = [c for c in critical if c in df.columns]
        completeness = float(df[existing].notna().mean().mean()) if existing else 0.0

        if "track_id" in df.columns and len(df):
            dup_share = 1 - (df["track_id"].nunique() / len(df))
        else:
            dup_share = 0.0

        if "popularity" in df.columns and len(df):
            valid = float(df["popularity"].between(0, 100).mean())
        else:
            valid = 0.0

        score = round((0.5 * completeness + 0.3 * (1 - dup_share) + 0.2 * valid), 3)
        return {
            "completeness": round(completeness, 3),
            "duplicates": round(dup_share, 3),
            "consistency": round(valid, 3),
            "score": score,
        }

    # ---------- outputs ----------
    def _write_outputs(self):
        df = pd.DataFrame(self.data)
        raw_path = self.output_dir / "raw" / f"tracks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(raw_path, index=False)
        self.logger.info(f"Wrote {len(df)} rows → {raw_path}")

    def _write_metadata_and_quality(self):
        q = self._quality_checks()
        total_req = max(1, self.stats["total_requests"])
        success_rate = round(self.stats["success"] / total_req, 3)

        meta = {
            "collection_info": {
                "end_time": datetime.now().isoformat(),
                "total_records": len(self.data),
                "success_rate": success_rate,
                "requests": self.stats,
            },
            "sources": self.stats["apis_used"],
            "variables": [
                "playlist", "track_id", "track_name", "artist_name",
                "album_name", "release_date", "popularity", "added_at",
            ],
            "quality": q,
        }
        with open(self.output_dir / "metadata" / "dataset_metadata.json", "w") as f:
            json.dump(meta, f, indent=2)

        report = {
            "summary": {
                "total_records": len(self.data),
                "collection_success_rate": success_rate,
                "overall_quality_score": q["score"],
            },
            "completeness": q["completeness"],
            "dup_share": q["duplicates"],
            "consistency_popularity_0_100": q["consistency"],
            "recommendations": self._recommendations(q),
        }
        with open("reports_quality_report.json", "w") as f:
            json.dump(report, f, indent=2)
        self.logger.info("Wrote metadata and quality report")

    def _recommendations(self, q):
        recs = []
        if q["duplicates"] > 0.05:
            recs.append("Increase de-duplication by track_id.")
        if q["completeness"] < 0.95:
            recs.append("Fetch missing fields or drop incomplete rows.")
        if q["consistency"] < 0.98:
            recs.append("Validate popularity bounds; remove outliers.")
        if not recs:
            recs.append("Quality acceptable; scale to more regions.")
        return recs


if __name__ == "__main__":
    SpotifyAgent("config.json").run()

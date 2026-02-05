
import tempfile
import shutil
import traceback,os
import django
os.environ.setdefault(
"DJANGO_SETTINGS_MODULE",
"analysis_portfolio.settings",  # <-- CHANGE THIS
)
django.setup()
from django.db import close_old_connections

from .match_event_fetcher import MatchEventFetcher


def process_season_worker(season_id, game_id, save_path):
    # season_id, game_id, save_path = args

    close_old_connections()

    cache_dir = None
    try:
        os.makedirs("D:/alt_cache",exist_ok=True)
        cache_dir = tempfile.mkdtemp(
            prefix=f"Season_{season_id}__{game_id}__",
            dir="D:/alt_cache",
        )

        fetcher = MatchEventFetcher(cache_dir, season_id, game_id)
        tracker, error_items, status = fetcher.fetch_game_data_for_the_season(save_path)

        return tracker, error_items,status

    except Exception:
        return (
            {},
            {
                "stage": "process_crash",
                "game_id": game_id,
                "error": traceback.format_exc(),
            },
            "error"
        )

    finally:
        close_old_connections()
        if cache_dir:
            shutil.rmtree(cache_dir, ignore_errors=True)

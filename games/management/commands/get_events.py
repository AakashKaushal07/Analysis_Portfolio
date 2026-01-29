from django.core.management.base import BaseCommand
from django.db import close_old_connections

from .utils.match_event_fetcher import MatchEventFetcher
from leagues.models import Season

import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

class Command(BaseCommand):
    help = "fetch events and shot data combined with momentume indo"

    def add_arguments(self, parser):
        parser.add_argument(
            '--workers', '-w', type=int, default=4,
            help='Number of parallel worker processes (default: 4)'
        )
        parser.add_argument(
            '--limit', type=int, default=None,
            help='Limit the number of seasons to process'
        )

    def _process_season(self, season_id):
            close_old_connections()

            cache_dir = tempfile.mkdtemp(
                prefix=f"Season_{season_id}__",
                dir="D:/alt_cache",
            )

            fetcher = MatchEventFetcher(cache_dir)
            tracker, error_items = fetcher.fetch_game_data_for_the_season(season_id)

            return season_id, tracker, error_items

    def handle(self, *args, **options):
        
        workers = options['workers']
        limit = options['limit']
        
        season_ids = Season.objects.all().values_list('id',flat=True)  # expand this later if needed
        if limit:
            season_ids = season_ids[:limit]

        overall_result = {}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(self._process_season, season_id)
                for season_id in season_ids
            ]

            for future in as_completed(futures):
                season_id, tracker, error_items = future.result()
                overall_result[season_id] = {
                    "tracker": tracker,
                    "errors": error_items,
                }

        self.stdout.write(
            self.style.SUCCESS(
                f"Completed processing {len(overall_result)} season(s)"
            )
        )
        with open("Sample Response.txt","w") as f :
            f.write(str(overall_result))
        # return overall_result
        ## Save Overall data in a text file for now, then on mail
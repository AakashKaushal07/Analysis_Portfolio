from django.core.management.base import BaseCommand
from .utils.game_fetch_help import season_link_maker
from leagues.models import Season
from multiprocessing import Pool
import os

# Change Tensolflow logs


class Command(BaseCommand):
    help = "Fetch all league fixtures and shots using multiprocessing."

    def add_arguments(self, parser):
        parser.add_argument(
            '--workers', '-w', type=int, default=4,
            help='Number of parallel worker processes (default: 4)'
        )
        parser.add_argument(
            '--limit', type=int, default=None,
            help='Limit the number of seasons to process'
        )

    def handle(self, *args, **options):
        
        workers = options['workers']
        limit = options['limit']
        print("OS WALA : ",os.environ.get("TF_CPP_MIN_LOG_LEVEL"))
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
        print("OS WALA : ",os.environ["TF_CPP_MIN_LOG_LEVEL"])

        # Collect season IDs to avoid pickling Django model instances
        qs = Season.objects.select_related("competition").all()
        if limit is not None:
            qs = qs[:limit]
        season_ids = list(qs.values_list('id', flat=True))

        self.stdout.write(self.style.NOTICE(f"Starting fetch for {len(season_ids)} seasons with {workers} workers..."))

        # Multiprocessing pool with starmap
        jobs = [(i, sid) for i, sid in enumerate(season_ids, start=1)]
        try :
            with Pool(workers) as pool:
                # Wrapper function: fetch_data_and_save_locally_wrapper(i, season_id)
                pool.starmap(season_link_maker, jobs)
        except Exception as e:
            print("MAIN ERR",e)
        self.stdout.write(self.style.SUCCESS("✅ All jobs completed."))

from django.core.management.base import BaseCommand
from .utils.game_fetch_help import get_event_links_only
from django.db.models import Q
from leagues.models import Season
from multiprocessing import Pool
import pandas as pd
import os


class Command(BaseCommand):
    help = "Fetch all matches shots url via an input template using multiprocessing."

    def add_arguments(self, parser):
        parser.add_argument(
            '--input', '-i', type=str, required=True,
            help='File which has info of the actions to run.'
        )
        parser.add_argument(
            '--workers', '-w', type=int, default=4,
            help='Number of parallel worker processes (default: 4)'
        )
        parser.add_argument(
            '--limit', type=int, default=None,
            help='Limit the number of seasons to process'
        )

    def handle(self, *args, **options):
        
        input = options['input']
        workers = options['workers']
        limit = options['limit']
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

        # Collect season IDs to avoid pickling Django model instances
        # qs = Season.objects.select_related("competition").all()
        try :
            if '.csv' in input :
                df = pd.read_csv(input)
            elif '.xlsx' in input : 
                df = pd.read_excel(input)
            else:
                raise Exception("Only allowed ifles are .xlsx or .csv")
            q = Q()
            for _, row in df.iterrows():
                q |= (
                    Q(competition__confederation=row["confederation"], competition__country=row["region"],competition__competition_name=row['competition']) &
                    (Q(name=row["season"]) | Q(name_fotmob=row["season"]))
                )
            qs = Season.objects.select_related("competition").filter(q)
        except Exception as e :
            print(e)
            return 
        
        if limit is not None:
            qs = qs[:limit]
        season_ids = list(qs.values_list('id', flat=True))

        self.stdout.write(self.style.NOTICE(f"Starting fetch for {len(season_ids)} seasons with {workers} workers..."))

        # Multiprocessing pool with starmap
        jobs = [(i, sid) for i, sid in enumerate(season_ids, start=1)]
        try :
            with Pool(workers) as pool:
                #Change the function here
                pool.starmap(get_event_links_only, jobs)
        except Exception as e:
            print("MAIN ERR",e)
        self.stdout.write(self.style.SUCCESS("✅ All jobs completed."))

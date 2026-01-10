from django.core.management.base import BaseCommand
from .utils.game_fetch_help import season_link_maker
from leagues.models import Season,Competition
from multiprocessing import Pool
import os
from datetime import datetime
from django.db.models import Q

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

    def __fetch_recent_seasons(self):
        pass

    def __get_recent_seasons_from_db(self) :
        
        comps = Competition.objects.all()
        current_date = datetime.now().date()
        results = []
        query = Q()
        for comp in comps :
            start = comp.season_start
            end = comp.season_end
            if start < end :
                results.append((comp.competition_name,str(current_date.year)))
                query |= Q(competition = comp, name__istartswith=str(current_date.year))
            else :
                if current_date.month>=start and current_date.month <= 12 :
                    results.append((comp.competition_name,str(current_date.year)))
                    query |= Q(competition = comp, name__istartswith=str(current_date.year))
                    
                else:
                    results.append((comp.competition_name,str(current_date.year - 1)))
                    query |= Q(competition = comp, name__istartswith=str(current_date.year - 1))
        qs =  Season.objects.select_related("competition").filter(query)   
        if abs(len(qs) - len(results)) > 5 :
            pass
            print("Bahut bada fuck up")
            # TODO : pull data   
        return qs         
        
    
    def handle(self, *args, **options):
        
        workers = options['workers']
        limit = options['limit']
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
        
        qs = self.__get_recent_seasons_from_db()
        season_ids = list(qs.values_list('id', flat=True))
        
        if limit is not None:
            season_ids = season_ids[:limit]

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

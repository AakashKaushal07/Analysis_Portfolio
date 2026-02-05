from django.core.management.base import BaseCommand
from django.db import close_old_connections
from django.db.models import Count, Q

from base_app.models import ConfigItems

# from .utils.match_event_fetcher import MatchEventFetcher
from .utils.worker_module import process_season_worker
from leagues.models import Season
from games.models import Game

import tempfile,os,json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time

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
        parser.add_argument(
            '--file', type=str, default=None,
            help='Input the file with competetion name and season name'
        )
        

    def __get_path_to_save_df(self,season): 
        base_save_location = ConfigItems.objects.get(key="PREPARED_EVENT_PATH").value
        
        s_name = season.name.replace("/","-").replace(" ","_")
        conf = season.competition.confederation.replace("/","-").replace(" ","_")
        country = season.competition.country.replace("/","-").replace(" ","_")
        comp_name = season.competition.competition_name.replace("/","-").replace(" ","_")
        
        os.makedirs(f"{base_save_location}", exist_ok=True)
        os.makedirs(f"{base_save_location}/{conf}", exist_ok=True)
        os.makedirs(f"{base_save_location}/{conf}/{country}", exist_ok=True)
        os.makedirs(f"{base_save_location}/{conf}/{country}/{comp_name}", exist_ok=True)
        os.makedirs(f"{base_save_location}/{conf}/{country}/{comp_name}/{s_name}", exist_ok=True)
        return f"{base_save_location}/{conf}/{country}/{comp_name}/{s_name}"
   
    # def _process_season(self, season_id, game_id,save_path):
    #         close_old_connections()
            
    #         cache_dir = tempfile.mkdtemp(
    #             prefix=f"Season_{season_id}__{game_id}__",
    #             dir="D:/alt_cache",
    #         )

    #         fetcher = MatchEventFetcher(cache_dir,season_id,game_id)
    #         tracker, error_items,status = fetcher.fetch_game_data_for_the_season(save_path)

    #         return tracker, error_items,status

    def handle(self, *args, **options):
        
        workers = options['workers']
        limit = options['limit']
        input_file = options['file']
        if input_file :
            if input_file.endswith(".csv"):
                df = pd.read_csv(input_file)
            elif input_file.endswith(".xlsx"):
                df = pd.read_excel(input_file)
            else:
                raise ValueError(f"Unsupported file format : {input_file}")
            
            records = (
                df[["competition", "season"]]
                .dropna()
                .drop_duplicates()
                .to_records(index=False)
            )
            q = Q()
            for comp, season in records:
                q |= Q(
                    competition__competition_name=comp,
                    name__in=[season],
                ) | Q(
                    competition__competition_name=comp,
                    name_fotmob__in=[season],
                )

            seasons = Season.objects.select_related("competition").filter(q)
        else:
            seasons = Season.objects.all()  # expand this later if needed
        # TEST ONLY
        # seasons = seasons[14:]   
        if limit:
            seasons = seasons[:limit]

        overall_result = {}
        
        for sns in seasons :
            start = time()
            games = Game.objects.filter(season=sns).exclude(event_status='completed')
            game_ids=games.values_list('id',flat=True)
            stats = games.aggregate( # pyright: ignore[reportAttributeAccessIssue]
                total_event_matches=Count(
                    'id',
                    filter=Q(game_event_url__isnull=False) & ~Q(game_event_url='')
                ),
                total_shot_matches=Count(
                    'id',
                    filter=Q(game_shot_url__isnull=False) & ~Q(game_shot_url='')
                ),
            )
            tracker = {**{
                "competition": sns.competition.competition_name,
                "name": sns.name,
                "event_matches_pulled": 0,
                "event_matches_transformed": 0,
                "shot_matches_pulled": 0,
                "merge_successes": 0,
                "merge_failure": 0,
                'save_success':0,
                'save_failure':0,
                'item_mutate':0
            }, **stats}

            error_items = []
            save_path = self.__get_path_to_save_df(sns)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(process_season_worker,sns.id,game_id,save_path) # type: ignore
                    for game_id in game_ids
                ]
                
                for future in as_completed(futures):
                    trk, err, status = future.result()
                    try :
                        Game.objects.filter(id=status.get('id','')).update(event_status = status.get('status','error'))
                    except Exception as e :
                        print(f"Exception for {status} | {e}")
                    if err != {}: 
                        error_items.append(err) 
                    tracker["event_matches_pulled"] += trk.get("event_matches_pulled",0)
                    tracker["event_matches_transformed"] += trk.get("event_matches_transformed",0)
                    tracker["shot_matches_pulled"] += trk.get("shot_matches_pulled",0)
                    tracker["merge_successes"] += trk.get("merge_successes",0)
                    tracker["merge_failure"] += trk.get("merge_failure",0)
                    tracker["save_success"] += trk.get("save_success",0)
                    tracker["save_failure"] += trk.get("save_failure",0)
                    tracker["item_mutate"] += trk.get("item_mutate", 0)
                    
            tracker['time_elapsed'] = time() - start      
            overall_result[str(sns)] ={'tracker' : tracker,'errors':error_items}
            with open(f"Response.json","w") as f :
                f.write(json.dumps(overall_result))
            self.stdout.write(
                self.style.SUCCESS(
                    f"Completed processing {str(sns)} season(s)"
                )
            )
        with open("Sample Response.txt","w") as f :
            f.write(str(overall_result))
        # return overall_result
        ## Save Overall data in a text file for now, then on mail
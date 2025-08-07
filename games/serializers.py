import re
from datetime import datetime

from rest_framework import serializers
from .models import Game
from leagues.models import Season,Competition

class LeagueLookupSerializer(serializers.Serializer):
    league_id = serializers.IntegerField(required=False)
    league_name = serializers.CharField(required=False)
    
    def validate_league_id(self, value):
        if not Competition.objects.filter(id=value).exists():
            raise serializers.ValidationError("League id not found")
        return value


    def validate_league_name(self, value):
        if not Competition.objects.filter(name=value).exists():
            raise serializers.ValidationError("League name not found")
        return value

class SeasonLookupSerializer(serializers.Serializer):
    season = serializers.CharField(required=False)
    season_from = serializers.CharField(required=False)
    season_to = serializers.CharField(required=False)

    def validate(self, data):
        all_years_value = data.values()
        if not all_years_value:
            return None
        print(all_years_value)
        for yr in all_years_value:
            match = re.match(r"^(?P<year>\d{4}(?:/\d{4})?)\s*[-]?\s*(?P<stage>.*)?$", yr.strip())
            if match:
                year = match.group("year")
                stage = match.group("stage").strip() if match.group("stage") else None
                print(f"Year: {year}, Stage: {stage}")
                try :
                    yr_dt = (datetime.strptime(yr_name, "%Y") for yr_name in year.split(r"/"))
                except Exception as e:
                    print(e)
                    raise serializers.ValidationError(f"Invalid year format: {year}. Expected format is YYYY or YYYY.")
        return data
                
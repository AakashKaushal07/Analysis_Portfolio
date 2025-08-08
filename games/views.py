from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import LeagueLookupSerializer,SeasonLookupSerializer
# from .utils as utils

def games_page(request):
    return render(request, 'games/games.html')

class get_game_url(APIView):
    def post(self, request):
        # league level checkup
        league_serializer = LeagueLookupSerializer(data=request.data)
        if not league_serializer.is_valid(): ## Error case
            return Response(league_serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        # seasons level checkup
        season_serializer = SeasonLookupSerializer(data=request.data)
        if not season_serializer.is_valid(): ## Error case
            return Response(season_serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        season_info = season_serializer.validated_data
        ## TODO :  Complete this after creating a proper format. Fill up games so that There is something to show for.
        return Response(season_serializer.validated_data, status=status.HTTP_410_GONE)
from django.shortcuts import render

def games_page(request):
    return render(request, 'games/games.html')

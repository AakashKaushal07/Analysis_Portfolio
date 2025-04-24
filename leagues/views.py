from django.shortcuts import render

def leagues_page(request):
    return render(request, 'leagues/leagues.html')

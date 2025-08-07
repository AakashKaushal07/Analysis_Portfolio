from django.urls import path
from . import views

urlpatterns = [
    path('', views.games_page, name='games-home'),
    path('get_game', views.get_game_url.as_view(), name='get_game'),
]

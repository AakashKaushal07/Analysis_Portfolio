from django.urls import path
from . import views

urlpatterns = [
    path('', views.games_page, name='leagues-home'),
]

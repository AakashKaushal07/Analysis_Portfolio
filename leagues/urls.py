from django.urls import path
from . import views

urlpatterns = [
    path('', views.leagues_page, name='leagues-home'),
]

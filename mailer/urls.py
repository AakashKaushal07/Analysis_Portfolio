from django.urls import path
from . import views

urlpatterns = [
    path('test_mail/', views.test_mail, name='test_mail'),
]
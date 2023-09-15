from django.urls import path

from . import views

urlpatterns = [
    path('', views.login_view, name='login'),
    path('result/', views.result_page, name='result'),
    path('bye/', views.bye_page, name='bye'),
    path('progress/', views.progress_page, name='progress'),
    path('check_progress/', views.check_progress, name='check_progress'),

]

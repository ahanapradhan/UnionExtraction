from django.urls import path

from . import views

urlpatterns = [
    path('', views.login_view, name='login'),
    path('result/', views.result_page, name='result'),
    path('bye/', views.bye_page, name='bye'),
]

from django.urls import path

from . import views

urlpatterns = [
    path('', views.login_view, name='login'),
    path('success/', views.success_page, name='success'),
    path('query/', views.query_page, name='query'),
    path('result/', views.result_page, name='result'),
    path('bye/', views.bye_page, name='bye'),
]

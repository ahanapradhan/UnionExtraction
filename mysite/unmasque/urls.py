from django.urls import register_converter, path
from . import views


class NegativeIntConverter:
    regex = '-?\d+'

    def to_python(self, value):
        return int(value)

    def to_url(self, value):
        return '%d' % value

register_converter(NegativeIntConverter, 'negint')

urlpatterns = [
    path('', views.login_view, name='login'),
    path('result/<negint:token>', views.result_page, name='result'),
    path('bye/', views.bye_page, name='bye'),
    path('progress/<negint:token>', views.progress_page, name='progress'),
    path('check_progress/<negint:token>', views.check_progress, name='check_progress'),
    path('cancel/<negint:token>', views.cancel_exec, name='cancel')
]

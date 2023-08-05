import psycopg2
from django.shortcuts import render, redirect
from psycopg2 import OperationalError

from .refactored.ConnectionHelper import ConnectionHelper
from .src.core import UnionPipeLine


# Create your views here.


def login_view(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        host = request.POST.get('host')
        port = request.POST.get('port')
        database = request.POST.get('database')
        try:
            conn = connect_to_db(database, host, password, port, username)
            print(conn)
            conn.close()
        except OperationalError:
            error_message = 'Invalid credentials. Please try again.'
            return render(request, 'unmasque/login.html', {'error_message': error_message})

        connHelper = ConnectionHelper(database, username, password, port, host)
        query = request.POST.get('query')
        data, tp = UnionPipeLine.extract(connHelper, query)
        request.session['partials'] = [query, data, tp.get_json_display_string()]
        return redirect('result')

    return render(request, 'unmasque/login.html')


def connect_to_db(database, host, password, port, username):
    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=host,
        port=port
    )
    return connection


def result_page(request):
    # Retrieve the result from the previous view through session
    partials = request.session.get('partials')
    print(partials)
    return render(request, 'unmasque/result.html', {'query': partials[0], 'result': partials[1],
                                                    'profiling': partials[2]})


def bye_page(request):
    return render(request, 'unmasque/bye.html')

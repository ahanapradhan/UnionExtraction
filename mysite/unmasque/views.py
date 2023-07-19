import psycopg2
from django.shortcuts import render, redirect
from psycopg2 import OperationalError

from .src.core import algorithm1
from .src.mocks.database import TPCH


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
            conn.close()
            return redirect('success')
        except OperationalError:
            error_message = 'Invalid credentials. Please try again.'
            return render(request, 'unmasque/login.html', {'error_message': error_message})

    return render(request, 'unmasque/login.html')


def connect_to_db(database, host, password, port, username):
    print(database)
    print(host)
    print(password)
    print(username)
    print(port)
    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=host,
        port=port
    )
    print(connection)
    return connection


def success_page(request):
    return render(request, 'unmasque/success.html')


def query_page(request):
    if request.method == 'POST':
        query = request.POST['query']
        db = TPCH()
        p, data = algorithm1.algo(db, query)
        return redirect('result', data=data)

    return render(request, 'unmasque/query.html')


def result_page(request):
    # Retrieve the result from the previous view or database
    data = request.GET.get('data')
    print(data)
    return render(request, 'unmasque/result.html', {'result': data})


import psycopg2
from django.shortcuts import render, redirect
from psycopg2 import OperationalError

from .src.pipeline.UnionPipeLine import UnionPipeLine
from .src.util.ConnectionHelper import ConnectionHelper


# Create your views here.


def login_view(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        host = request.POST.get('host')
        port = request.POST.get('port')
        database = request.POST.get('database')
        query = request.POST.get('query')
        try:
            conn = connect_to_db(database, host, password, port, username)
            print(conn)
            cur = conn.cursor()
            try:
                cur.execute("EXPLAIN " + query)
            except:
                error_message = "Invalid query. Please Try again!"
                conn.close()
                return render(request, 'unmasque/login.html', {'error_message': error_message})
            conn.close()
        except OperationalError:
            error_message = 'Invalid credentials. Please try again.'
            return render(request, 'unmasque/login.html', {'error_message': error_message})

        connHelper = ConnectionHelper(dbname=database, user=username, password=password, port=port, host=host)
        doExtraction(connHelper, query, request)
        return redirect('result')

    return render(request, 'unmasque/login.html')


def doExtraction(connHelper, query, request):
    pipeline = UnionPipeLine(connHelper)
    data = pipeline.extract(query)
    tp = pipeline.time_profile
    to_pass = [query]
    if data is not None:
        to_pass.append(data)
    else:
        to_pass.append("Sorry! Could not extract hidden query!")

    if tp is not None:
        to_pass.append(tp.get_json_display_string())
    else:
        to_pass.append("Nothing to show!")

    request.session['partials'] = to_pass


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

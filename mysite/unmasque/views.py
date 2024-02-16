import psycopg2
from django.http import JsonResponse
from django.shortcuts import render, redirect
from psycopg2 import OperationalError

from .src.pipeline.PipeLineFactory import PipeLineFactory
from .src.util.ConnectionHelper import ConnectionHelper
from .src.util.constants import DONE


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
        token = start_extraction_pipeline_async(connHelper, query, request)
        return redirect(f'progress/{token}')

    return render(request, 'unmasque/login.html')


def start_extraction_pipeline_async(connHelper, query, request):
    token = func_start(connHelper, query, request)
    return token


def func_start(connHelper, query, request):
    request.session['hq'] = query
    factory = PipeLineFactory()
    token = factory.doJobAsync(query, connHelper)
    request.session['token'] = token
    print("TOKEN: ", token)
    state_msg = factory.get_pipeline_state(token)
    print("State at init", state_msg)
    to_pass = [query, state_msg, 'NA']
    request.session[str(token)+'partials'] = to_pass
    return token


def check_progress(request, token):
    print("...Checking Progress...")
    state_changed, state_msg = func_check_progress(request, token)
    return JsonResponse({'state_changed': state_changed, 'progress_message': state_msg})


def func_check_progress(request, token):
    print("...HIT...HIT...HIT...")
    state_changed = False
    factory = PipeLineFactory()
    print("TOKEN: ", token)
    state_msg = factory.get_pipeline_state(token)
    print("...got...", state_msg, factory.get_pipeline_query(token))
    if state_msg == DONE:
        print("...done...")
        state_changed = True
        to_pass = prepare_result(factory.get_pipeline_query(token))
    else:
        print("... still doing...", state_msg)
        to_pass = [factory.get_pipeline_query(token), state_msg, 'NA']
    request.session[str(token)+'partials'] = to_pass
    return state_changed, state_msg


def prepare_result(query):
    factory = PipeLineFactory()
    data = factory.result
    tp = factory.pipeline.time_profile
    to_pass = [query]
    if data is not None:
        to_pass.append(data)
    else:
        to_pass.append("Sorry! Could not extract hidden query!")
    if tp is not None:
        to_pass.append(tp.get_json_display_string())
    else:
        to_pass.append("Nothing to show!")
    return to_pass


def prepare_result_1(query, data):
    factory = PipeLineFactory()
    tp = factory.pipeline.time_profile
    to_pass = [query]
    if data is not None:
        to_pass.append(data)
    else:
        to_pass.append("Sorry! Could not extract hidden query!")
    print("Time info", tp.get_json_display_string())
    if tp is not None:
        to_pass.append(tp.get_json_display_string())
    else:
        to_pass.append("Nothing to show!")
    return to_pass

def connect_to_db(database, host, password, port, username):
    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=host,
        port=port
    )
    return connection


def result_page(request, token):
    # Retrieve the result from the previous view through session
    partials = request.session.get('partials')
    factory = PipeLineFactory()
    data = None
    query = None
    for i in factory.results:
        if i[0] == token:
            query = i[1]    
            data = i[2]
            break
    print(query, data)
    partials = prepare_result_1(query, data)

    print(partials)
    return render(request, 'unmasque/result.html', {'query': partials[0], 'result': partials[1],
                                                    'profiling': partials[2]})


def progress_page(request, token):
    partials = request.session.get(str(token)+'partials')
    print("Partials", partials, str(token)+'partials')
    return render(request, 'unmasque/progress.html', {'query': partials[0], 'progress_message': partials[1],
                                                      'profiling': 'NA', 'token': token})


def bye_page(request):
    return render(request, 'unmasque/bye.html')
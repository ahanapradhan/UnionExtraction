from django.http import JsonResponse
from django.shortcuts import render, redirect

from .src.core.factory.PipeLineFactory import PipeLineFactory
from .src.util.ConnectionFactory import ConnectionHelperFactory
from .src.util.constants import WAITING, FROM_CLAUSE, START, DONE, RUNNING, SAMPLING, DB_MINIMIZATION, EQUALITY, \
    FILTER, \
    LIMIT, ORDER_BY, AGGREGATE, GROUP_BY, PROJECTION, RESULT_COMPARE, OK, UNION, WRONG, RESTORE_DB, INEQUALITY

ALL_STATES = [WAITING, UNION, FROM_CLAUSE, SAMPLING, DB_MINIMIZATION, FILTER, EQUALITY, INEQUALITY,
              PROJECTION, GROUP_BY, AGGREGATE, ORDER_BY, LIMIT, RESULT_COMPARE, DONE]


# Create your views here.

def login_view(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        host = request.POST.get('host')
        port = request.POST.get('port')
        database = request.POST.get('database')
        query = request.POST.get('query')
        connHelper = ConnectionHelperFactory().createConnectionHelper()

        msg = connHelper.test_connection()
        if msg != OK:
            return render(request, 'unmasque/login.html', {'error_message': msg})

        msg = connHelper.validate_query(query)
        if msg != OK:
            return render(request, 'unmasque/login.html', {'error_message': msg})

        # SET THE FOLLOWING PARAMS FROM UI CHECKBOX VALUES
        
        connHelper.config.detect_nep = (request.POST.get("NEP") == "NEP")
        connHelper.config.detect_union = (request.POST.get("union") == "Union")
        connHelper.config.use_cs2 = (request.POST.get("sampling") == "Sampling")
        connHelper.config.detect_or = (request.POST.get("Disjunction") == "Disjunction")
        connHelper.config.detect_oj = (request.POST.get("OuterJoin") == "OuterJoin")
        #print("Checkbox", connHelper.config.detect_nep, connHelper.config.detect_union, connHelper.config.use_cs2)

        token = start_extraction_pipeline_async(connHelper, query, request)
        return redirect(f'progress/{token}')

    

    return render(request, 'unmasque/login.html', {
        
    })


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
    request.session[str(token) + 'partials'] = to_pass
    return token


def check_progress(request, token):
    print("...Checking Progress...")
    state_changed, state_msg, info, io = func_check_progress(request, token)
    return JsonResponse({'state_changed': state_changed, 'progress_message': state_msg, 'state_info': info, 'io': io}, safe=False)


def func_check_progress(request, token):
    state_changed = False
    factory = PipeLineFactory()
    p = factory.get_pipeline_obj(token)
    print("TOKEN: ", token)
    state_msg = factory.get_pipeline_state(token)
    print("Current state: ", state_msg, factory.get_pipeline_query(token))
    if state_msg in [DONE, WRONG]:
        print("...done!")
        state_changed = True
        to_pass = prepare_result(factory.get_pipeline_query(token))
    else:
        print("... still doing...", state_msg)
        to_pass = [factory.get_pipeline_query(token), state_msg, 'NA']
    request.session[str(token) + 'partials'] = to_pass
    return state_changed, state_msg, p.info, p.IO if p else None


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


def cancel_exec(request, token):
    print("ID:", token)
    print("Trying to cancel")
    factory = PipeLineFactory()
    factory.cancel_pipeline_exec(token)
    return render(request, 'unmasque/login.html')


def prepare_result_1(query, data, token):
    factory = PipeLineFactory()
    p = factory.get_pipeline_obj(token)
    to_pass = [query]
    if not p:
        to_pass.append('None')
        to_pass.append('None')
        return to_pass
    tp = p.time_profile
    if data is not None:
        to_pass.append(data)
    else:
        to_pass.append("Sorry! Could not extract hidden query!")
    print("Time info", tp.get_json_display_string())
    if tp is not None:
        to_pass.append(tp.get_table_display_string())
    else:
        to_pass.append("Nothing to show!")
    return to_pass


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
    partials = prepare_result_1(query, data, token)
    print(partials)
    return render(request, 'unmasque/result.html', {'query': partials[0], 'result': partials[1],
                                                    'profiling': partials[2]})


def progress_page(request, token):
    partials = request.session.get(str(token) + 'partials')
    print("Partials", partials, str(token) + 'partials')
    chunk_size = 5  # Number of stages per row
    stages = ALL_STATES[1:][:-1]
    rows = [stages[i:i+chunk_size] for i in range(0, len(stages), chunk_size)]
    reversed_rows = [row[::-1] if index % 2 == 1 else row for index, row in enumerate(rows)]
    return render(request, 'unmasque/progress.html', {'query': partials[0] if partials else "Not Valid Query",
                                                      'progress_message': partials[1] if partials else "_QNF_",
                                                      'profiling': 'NA', 'token': token, 'states': ALL_STATES,
                                                      "start": START,
                                                      "running": RUNNING, "done": DONE, "union": UNION,
                                                      'stages': ALL_STATES[1:][:-1], 'rows': reversed_rows})


def bye_page(request):
    return render(request, 'unmasque/bye.html')

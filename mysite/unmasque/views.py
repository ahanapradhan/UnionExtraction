from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse

from .src.core import algorithm1
from .src.mocks.database import TPCH


def index(request):
    query = "(select * from part,orders) union all (select * from customer)"
    db = TPCH()
    p, pstr = algorithm1.algo(db, query)
    return HttpResponse(pstr)

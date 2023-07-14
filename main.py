# This is a sample Python script.
from flask import Flask, request

import algorithm1
from database import TPCH

app = Flask(__name__)


# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

@app.post("/extractUnion")
def extract_union():
    if request.is_json:
        query = request.get_json()
        db = TPCH()
        p = algorithm1.algo(db, query)
        result = {"result": "success"}
        return result, 201
    return {"error": "Request must be JSON"}, 415


@app.route('/')
def local_call():
    db = TPCH()
    p = algorithm1.algo(db, "(select * from part) union all (select * from customer)")
    result = []
    for e in p:
        result.append(str(e))
    return result


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    query = "(select * from part) union all (select * from customer)"
    db = TPCH()
    p = algorithm1.algo(db, query)
    return p


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    p = print_hi('PyCharm')
    print("...... Result...")
    for pe in p:
        x = pe
        print(x)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

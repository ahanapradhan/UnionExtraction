from core import algorithm1
from mocks.database import TPCH


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    query = "(select * from part,orders) union all (select * from customer)"
    db = TPCH()
    p, pstr = algorithm1.algo(db, query)
    return pstr


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    p = print_hi('PyCharm')
    print("...... Result...")
    print(p)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

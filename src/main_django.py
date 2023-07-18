from src.core import algorithm1
from src.mocks.database import TPCH


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

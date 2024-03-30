import unittest


class Delegate:
    def sample(self, param, param1):
        return f"Hello {param} and {param1}"


class Worker:
    def __init__(self):
        self.delegate = Delegate()

    def execute(self, f, *p):
        func = getattr(self.delegate, f)
        return func(*p)


class MyTestCase(unittest.TestCase):

    def test_something(self):
        delg = Worker()
        check = delg.execute("sample", "ahana", "bye")
        self.assertEqual("Hello ahana and bye", check)
        print(delg.execute("sample", "Apurbo", "Ray"))


if __name__ == '__main__':
    unittest.main()

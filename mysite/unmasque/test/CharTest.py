import random
import string
import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        n = 1000
        # Get all lowercase and uppercase letters
        letters = string.ascii_lowercase + string.ascii_uppercase

        # Generate additional characters (e.g., digits and symbols) to reach 1000 characters
        additional_chars = "!@#$%^&*()-_=+[{]};:'\",<.>/?"
        additional_chars += string.digits  # Add digits

        # Concatenate letters and additional characters
        all_chars = letters + additional_chars

        # Extract 1000 different single characters
        single_chars = list(set(all_chars))[:n]

        print(single_chars)
        single_chars = [chr(random.randint(32, 126)) for _ in range(n)]
        print(single_chars)
        self.assertEqual(len(single_chars), n)


if __name__ == '__main__':
    unittest.main()

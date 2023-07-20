from django.test import TestCase, Client
from django.urls import reverse


class LoginTestCase(TestCase):
    def setUp(self):
        self.client = Client()

    def test_login_success(self):
        response = self.client.post(
            reverse('login'),
            {'username': 'postgres', 'password': 'postgres', 'host': 'localhost', 'port': '5432', 'database': 'tpch'}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, '/unmasque/success/')

    def test_login_failure(self):
        response = self.client.post(
            reverse('login'),
            {'username': 'invalid', 'password': 'blablabla', 'host': 'nohost', 'port': '1111', 'database': 'mockdb'}
        )
        print(response.status_code)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed("error.html")
        self.assertContains(response, 'Invalid credentials. Please try again.')

    def test_query_algo(self):
        self.client.post(
            reverse('login'),
            {'username': 'postgres', 'password': 'postgres', 'host': 'localhost', 'port': '5432', 'database': 'tpch'}
        )
        response = self.client.post(
            reverse('query'),
            {'query': '(select * from orders) union all (select * from lineitem)'}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, '/unmasque/result/')

    def test_repeat_algo(self):
        self.client.post(
            reverse('login'),
            {'username': 'postgres', 'password': 'postgres', 'host': 'localhost', 'port': '5432', 'database': 'tpch'}
        )
        response = self.client.post(
            reverse('query'),
            {'query': '(select * from orders) union all (select * from lineitem)'}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, '/unmasque/result/')
        response = self.client.post(
            reverse('query'),
            {'query': '(select * from part) union all (select * from customer)'}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, '/unmasque/result/')

    def test_bye_page(self):
        response = self.client.get(reverse('bye'))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'unmasque/bye.html')

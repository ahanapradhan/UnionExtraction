from django.test import TestCase, Client
from django.urls import reverse


class LoginTestCase(TestCase):
    def setUp(self):
        self.client = Client()

    def test_login_success(self):
        response = self.client.post(
            reverse('login'),
            {'username': 'postgres', 'password': 'postgres',
             'host': 'localhost', 'port': '5432', 'database': 'tpch',
             'query': '(select * from part) union all (select * from orders)'}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, '/unmasque/result/')

    def test_login_failure(self):
        response = self.client.post(
            reverse('login'),
            {'username': 'invalid', 'password': 'blablabla',
             'host': 'nohost', 'port': '1111', 'database': 'mockdb',
             'query': '(select * from part) union all (select * from orders)'}
        )
        print(response.status_code)
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed("error.html")
        self.assertContains(response, 'Invalid credentials. Please try again.')

    def test_bye_page(self):
        response = self.client.get(reverse('bye'))
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'unmasque/bye.html')

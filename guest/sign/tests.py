from django.test import TestCase
from sign.models import Event, Guest
from django.contrib.auth.models import User

# Create your tests here.
class ModelTest(TestCase):
    def setUp(self):
        Event.objects.create(id=1, name="oneplus", status='True', limit=2000, address='shenzhen',
                             start_time='2016-08-31 02:00:00')

    def test_event_models(self):
        result = Event.objects.get(name='oneplus')
        self.assertEqual(result.address, "shenzhen1")
        self.assertTrue(result.status)


class IndexPageTest(TestCase):
    def test_index_page_renders_index_template(self):
        response = self.client.get('/index/')
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'index.html')


class LoginActionTest(TestCase):
    def setUp(self):
        User.objects.create_user('admin', 'admin@mail.com', 'admin123456')

    def test_add_admin(self):
        user = User.objects.get(username='admin')

        self.assertEqual(user.username, 'admin')

    def test_login_action_username_password_null(self):
        test_data = {'username': '', 'password': ''}

        response = self.client.post('/login_action/', data=test_data)
        self.assertEqual(response.status_code , 200)
        self.assertIn(b"username or password error!", response.content)

    def test_login_action_username_password_error(self):
        test_data = {'username': 'abc', 'password': '123'}

        response = self.client.post('/login_action/', data=test_data)
        self.assertEqual(response.status_code , 200)
        self.assertIn(b"username or password error!", response.content)

    def test_login_action_success(self):
        test_data = {'username': 'admin', 'password': 'admin123456'}

        response = self.client.post('/login_action/', data=test_data)
        self.assertEqual(response.status_code , 302)
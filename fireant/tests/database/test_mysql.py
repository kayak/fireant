from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    patch,
)

from pypika import Field

from fireant import (
    annually,
    daily,
    hourly,
    monthly,
    quarterly,
    weekly,
)
from fireant.database import MySQLDatabase


class TestMySQLDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mysql = MySQLDatabase(database='testdb')

    def test_defaults(self):
        self.assertEqual('localhost', self.mysql.host)
        self.assertEqual(3306, self.mysql.port)
        self.assertEqual('utf8mb4', self.mysql.charset)
        self.assertIsNone(self.mysql.user)
        self.assertIsNone(self.mysql.password)

    def test_connect(self):
        mock_pymysql = Mock()
        with patch.dict('sys.modules', pymysql=mock_pymysql):
            mock_pymysql.connect.return_value = 'OK'

            mysql = MySQLDatabase(database='test_database', host='test_host', port=1234,
                                  user='test_user', password='password')
            result = mysql.connect()

        self.assertEqual('OK', result)
        mock_pymysql.connect.assert_called_once_with(
            host='test_host', port=1234, db='test_database', charset='utf8mb4',
            user='test_user', password='password', cursorclass=ANY
        )

    def test_trunc_hour(self):
        result = self.mysql.trunc_date(Field('date'), hourly)

        self.assertEqual('dashmore.TRUNC("date",\'hour\')', str(result))

    def test_trunc_day(self):
        result = self.mysql.trunc_date(Field('date'), daily)

        self.assertEqual('dashmore.TRUNC("date",\'day\')', str(result))

    def test_trunc_week(self):
        result = self.mysql.trunc_date(Field('date'), weekly)

        self.assertEqual('dashmore.TRUNC("date",\'week\')', str(result))

    def test_trunc_month(self):
        result = self.mysql.trunc_date(Field('date'), monthly)

        self.assertEqual('dashmore.TRUNC("date",\'month\')', str(result))

    def test_trunc_quarter(self):
        result = self.mysql.trunc_date(Field('date'), quarterly)

        self.assertEqual('dashmore.TRUNC("date",\'quarter\')', str(result))

    def test_trunc_year(self):
        result = self.mysql.trunc_date(Field('date'), annually)

        self.assertEqual('dashmore.TRUNC("date",\'year\')', str(result))

    def test_date_add_hour(self):
        result = self.mysql.date_add(Field('date'), 'hour', 1)

        self.assertEqual('DATE_ADD("date",INTERVAL 1 HOUR)', str(result))

    def test_date_add_day(self):
        result = self.mysql.date_add(Field('date'), 'day', 1)

        self.assertEqual('DATE_ADD("date",INTERVAL 1 DAY)', str(result))

    def test_date_add_week(self):
        result = self.mysql.date_add(Field('date'), 'week', 1)

        self.assertEqual('DATE_ADD("date",INTERVAL 1 WEEK)', str(result))

    def test_date_add_month(self):
        result = self.mysql.date_add(Field('date'), 'month', 1)

        self.assertEqual('DATE_ADD("date",INTERVAL 1 MONTH)', str(result))

    def test_date_add_quarter(self):
        result = self.mysql.date_add(Field('date'), 'quarter', 1)

        self.assertEqual('DATE_ADD("date",INTERVAL 1 QUARTER)', str(result))

    def test_date_add_year(self):
        result = self.mysql.date_add(Field('date'), 'year', 1)

        self.assertEqual('DATE_ADD("date",INTERVAL 1 YEAR)', str(result))

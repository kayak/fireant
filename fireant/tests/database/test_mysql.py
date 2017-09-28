# coding: utf-8
from unittest import TestCase

from mock import patch, Mock, ANY
from pypika import Field

from fireant.database import MySQLDatabase


class TestMySQLDatabase(TestCase):

    def test_defaults(self):
        mysql = MySQLDatabase(database='testdb')

        self.assertEqual('localhost', mysql.host)
        self.assertEqual(3306, mysql.port)
        self.assertEqual('utf8mb4', mysql.charset)
        self.assertIsNone(mysql.user)
        self.assertIsNone(mysql.password)

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
        result = MySQLDatabase(database='testdb').trunc_date(Field('date'), 'hour')

        self.assertEqual('dashmore.TRUNC("date",\'hour\')', str(result))

    def test_trunc_day(self):
        result = MySQLDatabase(database='testdb').trunc_date(Field('date'), 'day')

        self.assertEqual('dashmore.TRUNC("date",\'day\')', str(result))

    def test_trunc_week(self):
        result = MySQLDatabase(database='testdb').trunc_date(Field('date'), 'week')

        self.assertEqual('dashmore.TRUNC("date",\'week\')', str(result))

    def test_trunc_month(self):
        result = MySQLDatabase(database='testdb').trunc_date(Field('date'), 'month')

        self.assertEqual('dashmore.TRUNC("date",\'month\')', str(result))

    def test_trunc_quarter(self):
        result = MySQLDatabase(database='testdb').trunc_date(Field('date'), 'quarter')

        self.assertEqual('dashmore.TRUNC("date",\'quarter\')', str(result))

    def test_trunc_year(self):
        result = MySQLDatabase(database='testdb').trunc_date(Field('date'), 'year')

        self.assertEqual('dashmore.TRUNC("date",\'year\')', str(result))

    def test_date_add_hour(self):
        result = MySQLDatabase(database='testdb').date_add('hour', 1, Field('date'))

        self.assertEqual('DATE_ADD("date",INTERVAL 1 HOUR)', str(result))

    def test_date_add_day(self):
        result = MySQLDatabase(database='testdb').date_add('day', 1, Field('date'))

        self.assertEqual('DATE_ADD("date",INTERVAL 1 DAY)', str(result))

    def test_date_add_week(self):
        result = MySQLDatabase(database='testdb').date_add('week', 1, Field('date'))

        self.assertEqual('DATE_ADD("date",INTERVAL 1 WEEK)', str(result))

    def test_date_add_month(self):
        result = MySQLDatabase(database='testdb').date_add('month', 1, Field('date'))

        self.assertEqual('DATE_ADD("date",INTERVAL 1 MONTH)', str(result))

    def test_date_add_quarter(self):
        result = MySQLDatabase(database='testdb').date_add('quarter', 1, Field('date'))

        self.assertEqual('DATE_ADD("date",INTERVAL 1 QUARTER)', str(result))

    def test_date_add_year(self):
        result = MySQLDatabase(database='testdb').date_add('year', 1, Field('date'))

        self.assertEqual('DATE_ADD("date",INTERVAL 1 YEAR)', str(result))

# coding: utf-8
from datetime import date
from unittest import TestCase

from fireant.slicer import EqualityFilter, EqualityOperator, ContainsFilter, ExcludesFilter, RangeFilter, WildcardFilter
from pypika import Table


class FilterTests(TestCase):
    test_table = Table('abc')

    def test_equality_filter_equals_number(self):
        eq_filter = EqualityFilter('test', EqualityOperator.eq, 1)

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo"=1', str(schemas))

    def test_equality_filter_equals_char(self):
        eq_filter = EqualityFilter('test', EqualityOperator.eq, 'a')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo"=\'a\'', str(schemas))

    def test_equality_filter_equals_date(self):
        eq_filter = EqualityFilter('test', EqualityOperator.eq, date(2000, 1, 1))

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo"=\'2000-01-01\'', str(schemas))

    def test_equality_filter_equals_string(self):
        eq_filter = EqualityFilter('test', EqualityOperator.eq, 'test')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo"=\'test\'', str(schemas))

    def test_contains_filter_numbers(self):
        eq_filter = ContainsFilter('test', [1, 2, 3])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" IN (1,2,3)', str(schemas))

    def test_contains_filter_characters(self):
        eq_filter = ContainsFilter('test', ['a', 'b', 'c'])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" IN (\'a\',\'b\',\'c\')', str(schemas))

    def test_contains_filter_dates(self):
        eq_filter = ContainsFilter('test', [date(2000, 1, 1), date(2000, 1, 2), date(2000, 1, 3)])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" IN (\'2000-01-01\',\'2000-01-02\',\'2000-01-03\')', str(schemas))

    def test_contains_filter_strings(self):
        eq_filter = ContainsFilter('test', ['abc', 'efg', 'hij'])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" IN (\'abc\',\'efg\',\'hij\')', str(schemas))

    def test_excludes_filter_numbers(self):
        eq_filter = ExcludesFilter('test', [1, 2, 3])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" NOT IN (1,2,3)', str(schemas))

    def test_excludes_filter_characters(self):
        eq_filter = ExcludesFilter('test', ['a', 'b', 'c'])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" NOT IN (\'a\',\'b\',\'c\')', str(schemas))

    def test_excludes_filter_dates(self):
        eq_filter = ExcludesFilter('test', [date(2000, 1, 1), date(2000, 1, 2), date(2000, 1, 3)])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" NOT IN (\'2000-01-01\',\'2000-01-02\',\'2000-01-03\')', str(schemas))

    def test_excludes_filter_strings(self):
        eq_filter = ExcludesFilter('test', ['abc', 'efg', 'hij'])

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" NOT IN (\'abc\',\'efg\',\'hij\')', str(schemas))

    def test_range_filter_numbers(self):
        eq_filter = RangeFilter('test', 0, 1)

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" BETWEEN 0 AND 1', str(schemas))

    def test_range_filter_characters(self):
        eq_filter = RangeFilter('test', 'A', 'Z')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" BETWEEN \'A\' AND \'Z\'', str(schemas))

    def test_range_filter_dates(self):
        eq_filter = RangeFilter('test', date(2000, 1, 1), date(2000, 12, 31))

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" BETWEEN \'2000-01-01\' AND \'2000-12-31\'', str(schemas))

    def test_range_filter_strings(self):
        eq_filter = RangeFilter('test', 'ABC', 'XYZ')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" BETWEEN \'ABC\' AND \'XYZ\'', str(schemas))

    def test_wildcard_filter_suffix(self):
        eq_filter = WildcardFilter('test', '%xyz')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" LIKE \'%xyz\'', str(schemas))

    def test_wildcard_filter_prefix(self):
        eq_filter = WildcardFilter('test', 'abc%')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" LIKE \'abc%\'', str(schemas))

    def test_wildcard_filter_circumfix(self):
        eq_filter = WildcardFilter('test', 'lm%no')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" LIKE \'lm%no\'', str(schemas))

    def test_wildcard_filter_infix(self):
        eq_filter = WildcardFilter('test', '%lmn%')

        schemas = eq_filter.schemas(self.test_table.foo)

        self.assertEqual('"foo" LIKE \'%lmn%\'', str(schemas))

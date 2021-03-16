from unittest import TestCase

import fireant


class APITests(TestCase):
    def test_package_exports_databases(self):
        with self.subTest("base class"):
            self.assertIn("Database", vars(fireant))

        for db in ("MySQL", "Vertica", "Redshift", "PostgreSQL", "MSSQL", "Snowflake"):
            with self.subTest(db):
                self.assertIn(db + "Database", vars(fireant))

    def test_package_exports_dataset(self):
        self.assertIn("DataSet", vars(fireant))

        for element in ("Join", "Field", "DataType"):
            with self.subTest(element):
                self.assertIn(element, vars(fireant))

    def test_package_exports_intervals(self):
        for element in (
            "hour",
            "day",
            "week",
            "month",
            "quarter",
            "year",
            "NumericInterval",
        ):
            with self.subTest(element):
                self.assertIn(element, vars(fireant))

    def test_package_exports_references(self):
        for element in (
            "DayOverDay",
            "WeekOverWeek",
            "MonthOverMonth",
            "QuarterOverQuarter",
            "YearOverYear",
            "DaysOverDays",
            "WeeksOverWeeks",
            "MonthsOverMonths",
            "QuartersOverQuarters",
            "YearsOverYears",
        ):
            with self.subTest(element):
                self.assertIn(element, vars(fireant))

    def test_package_exports_modifiers(self):
        for element in ("Rollup", "OmitFromRollup", "ResultSet"):
            with self.subTest(element):
                self.assertIn(element, vars(fireant))

    def test_package_exports_operations(self):
        self.assertIn("Operation", vars(fireant))

        for element in ("CumSum", "CumMean", "CumProd", "RollingMean", "Share"):
            with self.subTest(element):
                self.assertIn(element, vars(fireant))

    def test_package_exports_exceptions(self):
        for element in ("DataSetException", "DataSetFilterException"):
            with self.subTest(element):
                self.assertIn(element, vars(fireant))

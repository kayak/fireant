from unittest import TestCase

from fireant import (
    DaysOverDays,
    DayOverDay,
    WeeksOverWeeks,
    WeekOverWeek,
    MonthsOverMonths,
    MonthOverMonth,
    QuartersOverQuarters,
    QuarterOverQuarter,
    YearsOverYears,
    YearOverYear,
)


class CumulativeOperationTests(TestCase):
    def test_DaysOverDays_with_1_as_interval_is_equal_to_DayOverDay(self):
        self.assertEqual(DaysOverDays(1), DayOverDay)

    def test_DaysOverDays_returns_the_right_reference_type(self):
        reference_type = DaysOverDays(2)

        with self.subTest('has alias set'):
            self.assertEqual(reference_type.alias, 'do2d')
        with self.subTest('has label set'):
            self.assertEqual(reference_type.label, 'Do2D')
        with self.subTest('has interval set to 2'):
            self.assertEqual(reference_type.interval, 2)

    def test_WeeksOverWeeks_with_1_as_interval_is_equal_to_WeekOverWeek(self):
        self.assertEqual(WeeksOverWeeks(1), WeekOverWeek)

    def test_WeeksOverWeeks_returns_the_right_reference_type(self):
        reference_type = WeeksOverWeeks(2)

        with self.subTest('has alias set'):
            self.assertEqual(reference_type.alias, 'wo2w')
        with self.subTest('has label set'):
            self.assertEqual(reference_type.label, 'Wo2W')
        with self.subTest('has interval set to 2'):
            self.assertEqual(reference_type.interval, 2)

    def test_MonthsOverMonths_with_1_as_interval_is_equal_to_MonthOverMonth(self):
        self.assertEqual(MonthsOverMonths(1), MonthOverMonth)

    def test_MonthsOverMonths_returns_the_right_reference_type(self):
        reference_type = MonthsOverMonths(2)

        with self.subTest('has alias set'):
            self.assertEqual(reference_type.alias, 'mo2m')
        with self.subTest('has label set'):
            self.assertEqual(reference_type.label, 'Mo2M')
        with self.subTest('has interval set to 2'):
            self.assertEqual(reference_type.interval, 2)

    def test_QuartersOverQuarters_with_1_as_interval_is_equal_to_QuarterOverQuarter(self):
        self.assertEqual(QuartersOverQuarters(1), QuarterOverQuarter)

    def test_QuartersOverQuarters_returns_the_right_reference_type(self):
        reference_type = QuartersOverQuarters(2)

        with self.subTest('has alias set'):
            self.assertEqual(reference_type.alias, 'qo2q')
        with self.subTest('has label set'):
            self.assertEqual(reference_type.label, 'Qo2Q')
        with self.subTest('has interval set to 2'):
            self.assertEqual(reference_type.interval, 2)

    def test_YearsOverYears_with_1_as_interval_is_equal_to_YearOverYear(self):
        self.assertEqual(YearsOverYears(1), YearOverYear)

    def test_YearsOverYears_returns_the_right_reference_type(self):
        reference_type = YearsOverYears(2)

        with self.subTest('has alias set'):
            self.assertEqual(reference_type.alias, 'yo2y')
        with self.subTest('has label set'):
            self.assertEqual(reference_type.label, 'Yo2Y')
        with self.subTest('has interval set to 2'):
            self.assertEqual(reference_type.interval, 2)

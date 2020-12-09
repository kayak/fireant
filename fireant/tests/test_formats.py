from datetime import (
    date,
    datetime,
)
from unittest import TestCase

import numpy as np

from fireant import (
    DataType,
    Field,
    day,
    formats,
    hour,
    month,
    quarter,
    week,
    year,
)
from fireant.dataset.totals import (
    DATE_TOTALS,
    NUMBER_TOTALS,
    TEXT_TOTALS,
)

text_field = Field("text", None, data_type=DataType.text)
number_field = Field("number", None, data_type=DataType.number)
boolean_field = Field("boolean", None, data_type=DataType.boolean)
date_field = Field("date", None, data_type=DataType.date)


class SafeRawValueTests(TestCase):
    def test_none_returned_as_null_label(self):
        self.assertEqual("null", formats.safe_value(None))

    def test_nan_returned_as_null_label(self):
        self.assertEqual("null", formats.safe_value(np.nan))

    def test_inf_returned_as_inf_label(self):
        with self.subTest("positive inf"):
            self.assertEqual("inf", formats.safe_value(np.inf))
        with self.subTest("negative inf"):
            self.assertEqual("inf", formats.safe_value(-np.inf))

    def test_boolean_value_is_returned_as_self_lower_case(self):
        for value in (True, False):
            with self.subTest("using value=" + str(value)):
                self.assertEqual(str(value).lower(), formats.display_value(value, boolean_field))


class FormatRawValueTests(TestCase):
    def test_none_returned_as_none(self):
        self.assertIsNone(formats.raw_value(None, None))

    def test_nan_returned_as_none(self):
        self.assertIsNone(formats.raw_value(np.nan, None))

    def test_inf_returned_as_inf_label(self):
        with self.subTest("positive inf"):
            self.assertIsNone(formats.raw_value(np.inf, None))
        with self.subTest("negative inf"):
            self.assertIsNone(formats.raw_value(-np.inf, None))

    def test_totals_markers_are_returned_as_text_totals_marker(self):
        for marker, field in [
            (TEXT_TOTALS, text_field),
            (NUMBER_TOTALS, number_field),
            (DATE_TOTALS, date_field),
        ]:
            with self.subTest(field.data_type):
                self.assertEqual("$totals", formats.raw_value(marker, field))

    def test_text_value_is_returned_as_self(self):
        for value in ("abc", " dc23d- 0f30fi", ""):
            with self.subTest("using value" + value):
                self.assertEqual(value, formats.raw_value(value, text_field))

    def test_num_value_is_returned_as_self(self):
        for value in (0, -1, 1, 100, 0.0, -1.1, 1.1, 0.123456789, -0.123456789):
            with self.subTest("using value" + str(value)):
                self.assertEqual(value, formats.raw_value(value, number_field))

    def test_boolean_value_is_returned_as_self(self):
        for value in (True, False):
            with self.subTest("using value" + str(value)):
                self.assertEqual(value, formats.raw_value(value, boolean_field))

    def test_date_value_is_returned_as_iso_date_string(self):
        self.assertEqual("2019-01-01T00:00:00", formats.raw_value(date(2019, 1, 1), date_field))

    def test_datetime_value_is_returned_as_iso_date_string(self):
        self.assertEqual(
            "2019-01-01T12:30:02",
            formats.raw_value(datetime(2019, 1, 1, 12, 30, 2), date_field),
        )


class FormatDisplayValueTests(TestCase):
    def test_none_returned_as_nan_value_arg(self):
        for expected in ("", "abcdefg"):
            self.assertEqual(expected, formats.display_value(None, None, null_value=expected))
        self.assertEqual("null", formats.display_value(None, None))

    def test_nan_returned_as_string(self):
        for expected in ("", "abcdefg"):
            self.assertEqual(expected, formats.display_value(np.nan, None, nan_value=expected))
        self.assertEqual("NaN", formats.display_value(np.nan, None))

    def test_inf_returned_as_inf_label(self):
        with self.subTest("positive inf"):
            self.assertEqual("Inf", formats.display_value(np.inf, None))
        with self.subTest("negative inf"):
            self.assertEqual("Inf", formats.display_value(-np.inf, None))

    def test_wrong_type_passed_to_formatter_returns_value(self):
        result = formats.display_value("abcdef", number_field)
        self.assertEqual(result, "abcdef")

    def test_totals_markers_are_returned_as_text_totals_label(self):
        for marker, field in [
            (TEXT_TOTALS, text_field),
            (NUMBER_TOTALS, number_field),
            (DATE_TOTALS, date_field),
        ]:
            with self.subTest(field.data_type):
                self.assertEqual("Totals", formats.display_value(marker, field))

    def test_text_value_is_returned_as_none(self):
        for value in ("abc", " dc23d- 0f30fi", ""):
            with self.subTest("using value" + value):
                self.assertIsNone(formats.display_value(value, text_field))

    def test_int_value_is_returned_as_string_self(self):
        for value in (0, -1, 1, 100):
            with self.subTest("using value" + str(value)):
                self.assertEqual(str(value), formats.display_value(value, number_field))

    def test_decimal_value_is_returned_as_string_self_with_excess_zeroes_stripped_rounded_to_6_places(
        self,
    ):
        tests = [
            (0.0, "0"),
            (-1.1, "-1.1"),
            (1.1, "1.1"),
            (0.123456789, "0.123457"),
            (-0.123456789, "-0.123457"),
        ]
        for value, expected in tests:
            with self.subTest("using value" + str(value)):
                self.assertEqual(expected, formats.display_value(value, number_field))

    def test_boolean_value_is_returned_as_self_lower_case(self):
        for value in (True, False):
            with self.subTest("using value" + str(value)):
                self.assertEqual(str(value).lower(), formats.display_value(value, boolean_field))

    def test_date_value_with_no_interval_is_returned_as_date_string(self):
        for d in (date(2019, 1, 1), datetime(2019, 1, 1, 12, 30, 2)):
            with self.subTest("with " + d.__class__.__name__):
                self.assertEqual("2019-01-01", formats.display_value(d, date_field))

    def test_date_value_with_hour_interval_is_returned_as_date_string_to_the_minute_rounded_to_hour(
        self,
    ):
        self.assertEqual(
            "2019-01-01 12:00",
            formats.display_value(datetime(2019, 1, 1, 12, 30, 2), hour(date_field)),
        )

    def test_date_value_with_day_interval_is_returned_as_date_string_to_the_day(self):
        for d in (date(2019, 1, 1), datetime(2019, 1, 1, 12, 30, 2)):
            with self.subTest("with " + d.__class__.__name__):
                self.assertEqual("2019-01-01", formats.display_value(d, day(date_field)))

    def test_date_value_with_week_interval_is_returned_as_date_string_to_the_year_and_week_number(
        self,
    ):
        for d in (date(2019, 2, 25), datetime(2019, 2, 25, 12, 30, 2)):
            with self.subTest("with " + d.__class__.__name__):
                self.assertEqual("W08 2019-02-25", formats.display_value(d, week(date_field)))

    def test_date_value_with_month_interval_is_returned_as_date_string_to_the_month_and_year(
        self,
    ):
        for d in (date(2019, 1, 1), datetime(2019, 1, 1, 12, 30, 2)):
            with self.subTest("with " + d.__class__.__name__):
                self.assertEqual("Jan 2019", formats.display_value(d, month(date_field)))

    def test_date_value_with_quarter_interval_is_returned_as_date_string_to_the_quarter_and_year(self):
        dates = (date(2019, m, 1) for m in range(1, 13))
        result = [formats.display_value(d, quarter(date_field)) for d in dates]
        self.assertEqual(result, ['Q1 2019'] * 3 + ['Q2 2019'] * 3 + ['Q3 2019'] * 3 + ['Q4 2019'] * 3)

    def test_date_value_with_year_interval_is_returned_as_date_string_to_the_year(self):
        for d in (date(2019, 1, 1), datetime(2019, 1, 1, 12, 30, 2)):
            with self.subTest("with " + d.__class__.__name__):
                self.assertEqual("2019", formats.display_value(d, year(date_field)))


class FormatDisplayValueStyleTests(TestCase):
    def test_style_numbers_with_prefix(self):
        dollar_field = Field("number", None, data_type=DataType.number, prefix="$")
        self.assertEqual("$1", formats.display_value(1, dollar_field))

    def test_style_negative_numbers_with_prefix(self):
        dollar_field = Field("number", None, data_type=DataType.number, prefix="$")
        self.assertEqual("$-1", formats.display_value(-1, dollar_field))

    def test_style_numbers_with_suffix(self):
        euro_field = Field("number", None, data_type=DataType.number, suffix="€")
        self.assertEqual("1€", formats.display_value(1, euro_field))

    def test_style_numbers_with_precision(self):
        euro_field = Field("number", None, data_type=DataType.number, precision=2)
        self.assertEqual("1.23", formats.display_value(1.234567, euro_field))

    def test_style_numbers_with_thousands_separator(self):
        euro_field = Field("number", None, data_type=DataType.number, thousands=",")
        self.assertEqual("1,000,000", formats.display_value(1000000, euro_field))

    def test_style_numbers_with_mixed(self):
        euro_field = Field(
            "number",
            None,
            data_type=DataType.number,
            prefix="$",
            thousands=",",
            precision=2,
        )
        self.assertEqual("$-1,000,000.00", formats.display_value(-1000000, euro_field))

    def test_use_raw_value_int(self):
        field = Field("number", None, data_type=DataType.number, suffix="€", prefix="$")
        self.assertEqual("1234", formats.display_value(1234, field, use_raw_value=True))

    def test_use_raw_value_float(self):
        field = Field("number", None, data_type=DataType.number, suffix="€", prefix="$")
        self.assertEqual("1.546346346", formats.display_value(1.546346346, field, use_raw_value=True))

    def test_raw_value_formats_integers_with_trailing_zeros(self):
        field = Field("number", None, data_type=DataType.number)
        self.assertEqual("126500", formats.display_value(126500, field, use_raw_value=True))

    def test_raw_value_does_not_trim_zero_value(self):
        field = Field("number", None, data_type=DataType.number)
        self.assertEqual("0", formats.display_value(0, field, use_raw_value=True))

    def test_raw_value_when_precision_specified(self):
        field = Field("number", None, data_type=DataType.number, precision=4)
        self.assertEqual("1244996.1138", formats.display_value(1244996.1138000000000000, field, use_raw_value=True))

    def test_converts_percentage_to_decimal_when_use_raw_value_True_and_suffix_is_percentage(self):
        with self.subTest('when no precision'):
            field = Field("number", None, data_type=DataType.number, suffix="%")
            self.assertEqual("0.87123123131", formats.display_value(87.123123131, field, use_raw_value=True))

        with self.subTest('when precision'):
            field = Field("number", None, data_type=DataType.number, suffix="%", precision=2)
            self.assertEqual("0.0739", formats.display_value(7.38652, field, use_raw_value=True))

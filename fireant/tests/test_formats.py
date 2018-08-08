from unittest import (
    TestCase,
)

import numpy as np
import pandas as pd
from datetime import (
    date,
    datetime,
)

from fireant import formats


class FormatMetricValueTests(TestCase):
    def test_that_nan_data_point_is_convered_to_none(self):
        # np.nan is converted to None
        result = formats.metric_value(np.nan)
        self.assertIsNone(result)

    def test_that_inf_data_point_is_convered_to_none(self):
        # np.nan is converted to None
        result = formats.metric_value(np.inf)
        self.assertIsNone(result)

    def test_that_neg_inf_data_point_is_convered_to_none(self):
        # np.nan is converted to None
        result = formats.metric_value(-np.inf)
        self.assertIsNone(result)

    def test_str_data_point_is_returned_unchanged(self):
        result = formats.metric_value(u'abc')
        self.assertEqual('abc', result)

    def test_int64_data_point_is_returned_as_py_int(self):
        # Needs to be cast to python int
        result = formats.metric_value(np.int64(1))
        self.assertEqual(int(1), result)

    def test_data_data_point_is_returned_as_string_iso_no_time(self):
        # Needs to be converted to milliseconds
        result = formats.metric_value(date(2000, 1, 1))
        self.assertEqual('2000-01-01', result)

    def test_datatime_data_point_is_returned_as_string_iso_with_time(self):
        # Needs to be converted to milliseconds
        result = formats.metric_value(datetime(2000, 1, 1, 1))
        self.assertEqual('2000-01-01T01:00:00', result)

    def test_timestamp_no_time_data_point_is_returned_as_string_iso_no_time(self):
        # Needs to be converted to milliseconds
        result = formats.metric_value(pd.Timestamp(date(2000, 1, 1)))
        self.assertEqual('2000-01-01', result)

    def test_timestamp_data_point_is_returned_as_string_iso_no_time(self):
        # Needs to be converted to milliseconds
        result = formats.metric_value(pd.Timestamp(datetime(2000, 1, 1, 1)))
        self.assertEqual('2000-01-01T01:00:00', result)


class DisplayValueTests(TestCase):
    def test_str_value_no_formats(self):
        display = formats.metric_display('abcdef')
        self.assertEqual('abcdef', display)

    def test_bool_true_value_no_formats(self):
        display = formats.metric_display(True)
        self.assertEqual('true', display)

    def test_bool_false_value_no_formats(self):
        display = formats.metric_display(False)
        self.assertEqual('false', display)

    def test_int_value_no_formats(self):
        display = formats.metric_display(12345)
        self.assertEqual('12,345', display)

    def test_decimal_value_no_formats(self):
        display = formats.metric_display(12345.123456789)
        self.assertEqual('12,345.123457', display)

    def test_str_value_with_prefix(self):
        display = formats.metric_display('abcdef', prefix='$')
        self.assertEqual('$abcdef', display)

    def test_bool_true_value_with_prefix(self):
        display = formats.metric_display(True, prefix='$')
        self.assertEqual('$true', display)

    def test_bool_false_value_with_prefix(self):
        display = formats.metric_display(False, prefix='$')
        self.assertEqual('$false', display)

    def test_int_value_with_prefix(self):
        display = formats.metric_display(12345, prefix='$')
        self.assertEqual('$12,345', display)

    def test_decimal_value_with_prefix(self):
        display = formats.metric_display(12345.123456789, prefix='$')
        self.assertEqual('$12,345.123457', display)

    def test_str_value_with_suffix(self):
        display = formats.metric_display('abcdef', suffix='€')
        self.assertEqual('abcdef€', display)

    def test_bool_true_value_with_suffix(self):
        display = formats.metric_display(True, suffix='€')
        self.assertEqual('true€', display)

    def test_bool_false_value_with_suffix(self):
        display = formats.metric_display(False, suffix='€')
        self.assertEqual('false€', display)

    def test_int_value_with_suffix(self):
        display = formats.metric_display(12345, suffix='€')
        self.assertEqual('12,345€', display)

    def test_decimal_value_with_suffix(self):
        display = formats.metric_display(12345.123456789, suffix='€')
        self.assertEqual('12,345.123457€', display)

    def test_str_value_with_precision(self):
        display = formats.metric_display('abcdef', precision=2)
        self.assertEqual('abcdef', display)

    def test_bool_true_value_with_precision(self):
        display = formats.metric_display(True, precision=2)
        self.assertEqual('true', display)

    def test_bool_false_value_with_precision(self):
        display = formats.metric_display(False, precision=2)
        self.assertEqual('false', display)

    def test_int_value_with_precision(self):
        display = formats.metric_display(12345, precision=2)
        self.assertEqual('12,345', display)

    def test_decimal_value_with_precision_0(self):
        display = formats.metric_display(12345.123456789, precision=0)
        self.assertEqual('12,345', display)

    def test_decimal_value_with_precision_2(self):
        display = formats.metric_display(12345.123456789, precision=2)
        self.assertEqual('12,345.12', display)

    def test_decimal_value_with_precision_9(self):
        display = formats.metric_display(12345.123456789, precision=9)
        self.assertEqual('12,345.123456789', display)

    def test_decimal_value_with_precision_trim_trailing_zeros(self):
        result = formats.metric_display(1.01)
        self.assertEqual('1.01', result)


class CoerceTypeTests(TestCase):
    def allow_literal_nan(self):
        result = formats.coerce_type('nan')
        self.assertEqual('nan', result)

    def allow_literal_nan_upper(self):
        result = formats.coerce_type('NAN')
        self.assertEqual('NAN', result)
    def allow_literal_inf(self):
        result = formats.coerce_type('inf')
        self.assertEqual('inf', result)

    def allow_literal_inf_upper(self):
        result = formats.coerce_type('INF')
        self.assertEqual('INF', result)

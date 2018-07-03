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
    def test_precision_default(self):
        result = formats.metric_display(0.123456789)
        self.assertEqual('0.123457', result)

    def test_zero_precision(self):
        result = formats.metric_display(0.123456789, precision=0)
        self.assertEqual('0', result)

    def test_precision(self):
        result = formats.metric_display(0.123456789, precision=2)
        self.assertEqual('0.12', result)

    def test_precision_zero(self):
        result = formats.metric_display(0.0)
        self.assertEqual('0', result)

    def test_precision_trim_trailing_zeros(self):
        result = formats.metric_display(1.01)
        self.assertEqual('1.01', result)

    def test_prefix(self):
        result = formats.metric_display(0.12, prefix='$')
        self.assertEqual('$0.12', result)

    def test_suffix(self):
        result = formats.metric_display(0.12, suffix='€')
        self.assertEqual('0.12€', result)


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

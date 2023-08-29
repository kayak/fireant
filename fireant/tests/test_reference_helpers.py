from unittest import TestCase

import pandas as pd
from pypika import Table

from fireant import DataSet, DataType, Field
from fireant.dataset.filters import ComparisonOperator
from fireant.dataset.references import DayOverDay, ReferenceFilter
from fireant.reference_helpers import apply_reference_filters
from fireant.tests.database.mock_database import MockDatabase


class ReferenceFilterTests(TestCase):
    @classmethod
    def setUpClass(cls):
        db = MockDatabase()
        t0 = Table("test0")
        cls.dataset = DataSet(
            table=t0,
            database=db,
            fields=[
                Field(
                    "timestamp",
                    label="Timestamp",
                    definition=t0.timestamp,
                    data_type=DataType.date,
                ),
                Field(
                    "metric0",
                    label="Metric0",
                    definition=t0.metric,
                    data_type=DataType.number,
                ),
                Field(
                    "unused_metric",
                    label="Unused Metric",
                    definition=t0.unused_metric,
                    data_type=DataType.number,
                ),
            ],
        )

        cls.df = pd.DataFrame.from_dict({"$metric0": [1, 2, 3, 4], "$metric0_dod": [1, 5, 9, 12]})

    def test_reference_filter_with_greater_than(self):
        reference_filter = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.gt, 5)
        reference = DayOverDay(self.dataset.fields.timestamp, filters=[reference_filter])

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            pd.DataFrame.from_dict({"$metric0": [3, 4], "$metric0_dod": [9, 12]}),
        )

    def test_reference_filter_with_greater_than_or_equal(self):
        reference_filter = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.gte, 5)
        reference = DayOverDay(self.dataset.fields.timestamp, filters=[reference_filter])

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            pd.DataFrame.from_dict({"$metric0": [2, 3, 4], "$metric0_dod": [5, 9, 12]}),
        )

    def test_reference_filter_with_less_than(self):
        reference_filter = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.lt, 5)
        reference = DayOverDay(self.dataset.fields.timestamp, filters=[reference_filter])

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            pd.DataFrame.from_dict({"$metric0": [1], "$metric0_dod": [1]}),
        )

    def test_reference_filter_with_less_than_or_equal(self):
        reference_filter = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.lte, 5)
        reference = DayOverDay(self.dataset.fields.timestamp, filters=[reference_filter])

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            pd.DataFrame.from_dict({"$metric0": [1, 2], "$metric0_dod": [1, 5]}),
        )

    def test_reference_filter_with_equal_to(self):
        reference_filter = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.eq, 5)
        reference = DayOverDay(self.dataset.fields.timestamp, filters=[reference_filter])

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            pd.DataFrame.from_dict({"$metric0": [2], "$metric0_dod": [5]}),
        )

    def test_reference_filter_with_not_equal_to(self):
        reference_filter = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.ne, 5)
        reference = DayOverDay(self.dataset.fields.timestamp, filters=[reference_filter])

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            pd.DataFrame.from_dict({"$metric0": [1, 3, 4], "$metric0_dod": [1, 9, 12]}),
        )

    def test_reference_filter_with_2_filters_combined(self):
        reference_filter_gt = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.gt, 3)
        reference_filter_lt = ReferenceFilter(self.dataset.fields.metric0, ComparisonOperator.lt, 10)
        reference = DayOverDay(
            self.dataset.fields.timestamp,
            filters=[reference_filter_gt, reference_filter_lt],
        )

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(
            result_df.reset_index(drop=True),
            pd.DataFrame.from_dict({"$metric0": [2, 3], "$metric0_dod": [5, 9]}),
        )

    def test_reference_filter_for_an_unselected_metric(self):
        reference_filter = ReferenceFilter(self.dataset.fields.unused_metric, ComparisonOperator.gt, 5)
        reference = DayOverDay(self.dataset.fields.timestamp, filters=[reference_filter])

        result_df = apply_reference_filters(self.df, reference)

        pd.testing.assert_frame_equal(result_df, self.df)

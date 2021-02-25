import pandas as pd

from unittest import TestCase

from fireant.tests.dataset.mocks import (
    dimx2_date_str_df,
    mock_dataset,
)
from fireant.widgets.base import Widget
from fireant.utils import alias_selector


class ReactTableTransformerTests(TestCase):
    maxDiff = None

    def test_hide_data_frame_indexes_hides_found_aliases(self):
        widget = Widget()

        base_df = dimx2_date_str_df.copy()[[alias_selector('wins'), alias_selector('votes')]]
        result = base_df.copy()
        widget.hide_data_frame_indexes(
            result,
            [
                alias_selector(mock_dataset.fields.political_party.alias),
                alias_selector(mock_dataset.fields.votes.alias),
                alias_selector('unknown'),
            ],
        )

        expected = base_df.copy()
        expected.reset_index('$political_party', inplace=True, drop=True)
        del expected['$votes']

        pd.testing.assert_frame_equal(expected, result)

    def test_hidden_aliases(self):
        widget = Widget(hide=[mock_dataset.fields.political_party, mock_dataset.fields.votes, 'field_x'])

        transform_dimensions = [mock_dataset.fields['candidate-name']]
        self.assertSetEqual(
            widget.hide_aliases(transform_dimensions),
            {'$political_party', '$votes', '$field_x'},
        )

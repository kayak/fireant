# coding: utf-8
import unittest
from datetime import date

import numpy as np
import pandas as pd


def _rollup(data_frame, levels):
    roll = data_frame.groupby(level=levels).sum()
    rolled_levels = [name
                     for i, name in enumerate(data_frame.index.names)
                     if i not in levels]

    for rolled_level in rolled_levels:
        roll[rolled_level] = None

    return data_frame.append(roll.set_index(rolled_levels, append=True)).sort_index()


class BaseTransformerTests(unittest.TestCase):
    cont_dim = {'label': 'Cont', 'id_fields': ['cont']}
    time_dim = {'label': 'Date', 'id_fields': ['dt']}
    uni1_dim = {'label': 'Uni1', 'id_fields': ['uni1_id'], 'label_field': 'uni1_label'}
    uni2_dim = {'label': 'Uni2', 'id_fields': ['uni2_id0', 'uni2_id1'], 'label_field': 'uni2_label'}
    cat1_dim = {'label': 'Cat1', 'id_fields': ['cat1'], 'label_options': {'a': 'A', 'b': 'B'}}
    cat2_dim = {'label': 'Cat2', 'id_fields': ['cat2'], 'label_options': {'y': 'Y', 'z': 'Z'}}

    shortcuts = {
        'a': 'A',
        'b': 'B',
        'c': 'C',
        'd': 'D',
        'y': 'Y',
        'z': 'Z',
        np.nan: 'Total',
    }

    cont_idx = pd.Index([0, 1, 2, 3, 4, 5, 6, 7], name='cont')
    cat1_idx = pd.Index(['a', 'b'], name='cat1')
    cat2_idx = pd.Index(['y', 'z'], name='cat2')

    uni1_idx = pd.MultiIndex.from_tuples([('Uni1_1', 1), ('Uni1_2', 2), ('Uni1_3', 3)],
                                         names=['uni1_label', 'uni1_id'])
    uni2_idx = pd.MultiIndex.from_tuples([('Uni2_1', 1, 100), ('Uni2_2', 2, 200), ('Uni2_3', 3, 300),
                                          ('Uni2_4', 4, 400)], names=['uni2_label', 'uni2_id0', 'uni2_id1'])

    datetime_idx = pd.DatetimeIndex(pd.date_range(start=date(2000, 1, 1), periods=8), name='dt')
    cont_cat_idx = pd.MultiIndex.from_product([cont_idx, cat1_idx], names=['cont', 'cat1'])
    cont_uni_idx = pd.MultiIndex.from_product([cont_idx, uni2_idx.levels[0]],
                                              names=['cont', 'uni2_label'])
    cat_cat_idx = pd.MultiIndex.from_product([cat1_idx, cat2_idx], names=['cat1', 'cat2'])
    cont_cat_cat_idx = pd.MultiIndex.from_product([cont_idx, cat1_idx, cat2_idx], names=['cont', 'cat1', 'cat2'])

    cont_cat_uni_idx = pd.MultiIndex.from_product([cont_idx, cat1_idx, uni2_idx.levels[0]],
                                                  names=['cont', 'cat1', 'uni2_label'])

    # Test DF with single continuous dimension and one metric column
    no_dims_multi_metric_df = pd.DataFrame(
        [[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7]],
        columns=['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight'],
    )
    no_dims_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two', 'three': 'Three', 'four': 'Four',
                    'five': 'Five', 'six': 'Six', 'seven': 'Seven', 'eight': 'Eight'},
        'dimensions': []
    }

    # Test DF with single continuous dimension and one metric column
    cont_dim_single_metric_df = pd.DataFrame(
        list(zip([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7])),
        columns=['one'],
        index=cont_idx
    )
    cont_dim_single_metric_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [cont_dim]
    }

    # Test DF with single continuous dimension and two metric columns
    cont_dim_multi_metric_df = pd.DataFrame(
        list(zip([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
                 [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7])),
        columns=['one', 'two'],
        index=cont_idx
    )
    cont_dim_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cont_dim]
    }

    # Test DF with single key unique dimension and one metric column
    uni_dim_single_metric_df = pd.DataFrame(
        list(zip([0.0, 0.1, 0.2])),
        columns=['one'],
        index=uni1_idx
    )
    uni_dim_single_metric_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [uni1_dim]
    }

    # Test DF with composite key unique dimension and two metric columns
    uni_dim_multi_metric_df = pd.DataFrame(
        list(zip([0.0, 0.1, 0.2, 0.3],
                 [1.0, 1.1, 1.2, 0.4])),
        columns=['one', 'two'],
        index=uni2_idx
    )
    uni_dim_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [uni2_dim]
    }

    # Test DF with single categorical dimension and one metric column
    cat_dim_single_metric_df = pd.DataFrame(
        list(zip([0.0, 1.0])),
        columns=['one'],
        index=cat1_idx
    )
    cat_dim_single_metric_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [cat1_dim]
    }

    # Test DF with single categorical dimension and two metric columns
    cat_dim_multi_metric_df = pd.DataFrame(
        list(zip([0.0, 1.0],
                 [10.0, 20.0])),
        columns=['one', 'two'],
        index=cat1_idx
    )
    cat_dim_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cat1_dim]
    }

    # Test DF with single continuous time dimension and one metric column
    time_dim_single_metric_df = pd.DataFrame(
        list(zip([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7])),
        columns=['one'],
        index=datetime_idx
    )
    time_dim_single_metric_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [time_dim]
    }
    time_dim_single_metric_ref_df = pd.DataFrame(
        np.random.randint(0, 10, size=(8, 2)),
        columns=[['', 'wow'], ['one', 'one']],
        index=datetime_idx
    )
    time_dim_single_metric_ref_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [time_dim],
        'references': {'wow': 'WoW'}
    }

    # Test DF with continuous and categorical dimensions and one metric column
    cont_cat_dims_single_metric_df = pd.DataFrame(
        np.matrix(np.arange(1, 17)).T,
        columns=['one'],
        index=cont_cat_idx
    )
    cont_cat_dims_single_metric_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [cont_dim, cat1_dim]
    }

    # Test DF with continuous and categorical dimensions and two metric columns
    cont_cat_dims_multi_metric_df = pd.DataFrame(
        np.matrix(np.arange(1, 17)).T * np.power(10, np.matrix(np.arange(0, 2))),
        columns=['one', 'two'],
        index=cont_cat_idx
    )
    cont_cat_dims_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cont_dim, cat1_dim]
    }

    # Test DF with continuous and unique dimensions and two metric columns
    _cont_uni = pd.DataFrame(
        np.array([np.arange(32), np.arange(100, 132)]).T,
        columns=['one', 'two'],
        index=cont_uni_idx
    )
    _cont_uni['uni2_id0'], _cont_uni['uni2_id1'] = None, None
    for label, id0, id1 in uni2_idx:
        _cont_uni.loc[pd.IndexSlice[:, label], ['uni2_id0', 'uni2_id1']] = id0, id1
    cont_uni_dims_multi_metric_df = _cont_uni.set_index(['uni2_id0', 'uni2_id1'], append=True)
    cont_uni_dims_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cont_dim, uni2_dim]
    }

    cont_uni_dims_single_metric_df = pd.DataFrame(cont_uni_dims_multi_metric_df['one'])
    cont_uni_dims_single_metric_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [cont_dim, uni2_dim]
    }

    # Test DF with two categorical dimensions and one metric column
    cat_cat_dims_single_metric_df = pd.DataFrame(
        [1, 2, 3, 4],
        columns=['one'],
        index=cat_cat_idx,
    )
    cat_cat_dims_single_metric_schema = {
        'metrics': {'one': 'One'},
        'dimensions': [cat1_dim, cat2_dim]
    }

    # Test DF with two categorical dimensions and two metric columns
    cat_cat_dims_multi_metric_df = pd.DataFrame(
        [[1, 10],
         [2, 20],
         [3, 30],
         [4, 40]],
        columns=['one', 'two'],
        index=cat_cat_idx,
    )
    cat_cat_dims_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cat1_dim, cat2_dim]
    }

    # Test DF with continuous and two categorical dimensions and two metric columns
    cont_cat_cat_dims_multi_metric_df = pd.DataFrame(
        np.matrix(np.arange(1, 33)).T * np.power(10, np.matrix(np.arange(0, 2))),
        columns=['one', 'two'],
        index=cont_cat_cat_idx
    )
    cont_cat_cat_dims_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cont_dim, cat1_dim, cat2_dim]
    }

    # Test DF with continuous and two categorical dimensions and two metric columns
    _cont_cat_uni = pd.DataFrame(
        np.array([np.arange(64), np.arange(100, 164)]).T,
        columns=['one', 'two'],
        index=cont_cat_uni_idx
    )
    _cont_cat_uni['uni2_id0'], _cont_cat_uni['uni2_id1'] = None, None
    for label, id0, id1 in uni2_idx:
        _cont_cat_uni.loc[pd.IndexSlice[:, :, label], ['uni2_id0', 'uni2_id1']] = id0, id1
    cont_cat_uni_dims_multi_metric_df = _cont_cat_uni.set_index(['uni2_id0', 'uni2_id1'], append=True)
    cont_cat_uni_dims_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cont_dim, cat1_dim, uni2_dim]
    }

    # Test DF with continuous and two categorical dimensions and two metric columns using rollup for totals
    rollup_cont_cat_cat_dims_multi_metric_df = pd.DataFrame(
        np.matrix(np.arange(1, 33)).T * np.power(10, np.matrix(np.arange(0, 2))),
        columns=['one', 'two'],
        index=cont_cat_cat_idx
    )
    rollup_cont_cat_cat_dims_multi_metric_df = _rollup(rollup_cont_cat_cat_dims_multi_metric_df, [0, 1])
    rollup_cont_cat_cat_dims_multi_metric_df = _rollup(rollup_cont_cat_cat_dims_multi_metric_df, [0])
    rollup_cont_cat_cat_dims_multi_metric_schema = {
        'metrics': {'one': 'One', 'two': 'Two'},
        'dimensions': [cont_dim, cat1_dim, cat2_dim]
    }

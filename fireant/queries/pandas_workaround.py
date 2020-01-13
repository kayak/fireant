import pandas as pd


def _reindex_with_nans(df, idx, fill_value=None):
    missing = pd.DataFrame(index=idx.symmetric_difference(df.index), columns=df.columns)
    if fill_value is not None:
        missing = missing.fillna(fill_value)
    return df.append(missing).reindex(idx)


def _reindex_deduplicate(left, right, fill_value=None):
    combined_index = left.index.append(right.index)
    dededuplicated_index = combined_index[~combined_index.duplicated()]
    left_reindex, right_reindex = [
        _reindex_with_nans(df, dededuplicated_index, fill_value=fill_value)
        for df in (left, right)
    ]
    return left_reindex, right_reindex


def df_subtract(left, right, fill_value=None):
    left_reindex, right_reindex = _reindex_deduplicate(
        left, right, fill_value=fill_value
    )
    return left_reindex.subtract(right_reindex)

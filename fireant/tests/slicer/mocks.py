from collections import (
    OrderedDict,
)
from datetime import (
    datetime,
)
from unittest.mock import Mock

import pandas as pd
from pypika import (
    JoinType,
    Table,
    functions as fn,
)

from fireant import *
from fireant.slicer.references import ReferenceType
from fireant.utils import (
    format_dimension_key as fd,
    format_metric_key as fm,
)


class TestDatabase(VerticaDatabase):
    # Vertica client that uses the vertica_python driver.

    connect = Mock()

    def __eq__(self, other):
        return isinstance(other, TestDatabase)


test_database = TestDatabase()
politicians_table = Table('politician', schema='politics')
voters_table = Table('voter', schema='politics')
state_table = Table('state', schema='locations')
district_table = Table('district', schema='locations')
deep_join_table = Table('deep', schema='test')

slicer = Slicer(
      table=politicians_table,
      database=test_database,

      joins=(
          Join(table=district_table,
               criterion=politicians_table.district_id == district_table.id,
               join_type=JoinType.outer),
          Join(table=district_table,
               criterion=politicians_table.district_id == district_table.id,
               join_type=JoinType.outer),
          Join(table=state_table,
               criterion=district_table.state_id == state_table.id),
          Join(table=voters_table,
               criterion=politicians_table.id == voters_table.politician_id),
          Join(table=deep_join_table,
               criterion=deep_join_table.id == state_table.ref_id),
      ),

      dimensions=(
          DatetimeDimension('timestamp',
                            label='Timestamp',
                            definition=politicians_table.timestamp),
          CategoricalDimension('political_party',
                               label='Party',
                               definition=politicians_table.political_party,
                               display_values=(
                                   ('d', 'Democrat'),
                                   ('r', 'Republican'),
                                   ('i', 'Independent'),
                                   ('l', 'Libertarian'),
                                   ('g', 'Green'),
                                   ('c', 'Constitution'))),
          UniqueDimension('candidate',
                          label='Candidate',
                          definition=politicians_table.candidate_id,
                          display_definition=politicians_table.candidate_name),
          UniqueDimension('election',
                          label='Election',
                          definition=politicians_table.election_id,
                          display_definition=politicians_table.election_year),
          UniqueDimension('district',
                          label='District',
                          definition=politicians_table.district_id,
                          display_definition=district_table.district_name),
          UniqueDimension('state',
                          label='State',
                          definition=district_table.state_id,
                          display_definition=state_table.state_name),
          BooleanDimension('winner',
                           label='Winner',
                           definition=politicians_table.is_winner),
          UniqueDimension('deepjoin',
                          definition=deep_join_table.id),
          PatternDimension('pattern',
                           label='Pattern',
                           definition=politicians_table.pattern),
      ),

      metrics=(
          Metric('votes',
                 label='Votes',
                 definition=fn.Sum(politicians_table.votes)),
          Metric('wins',
                 label='Wins',
                 definition=fn.Sum(politicians_table.is_winner)),
          Metric('voters',
                 label='Voters',
                 definition=fn.Count(voters_table.id)),
          Metric('turnout',
                 label='Turnout',
                 definition=fn.Sum(politicians_table.votes) / fn.Count(voters_table.id)),
      ),
)

political_parties = OrderedDict((('d', 'Democrat'),
                                 ('r', 'Republican'),
                                 ('i', 'Independent'),
                                 ('l', 'Libertarian'),
                                 ('g', 'Green'),
                                 ('c', 'Constitution')))

candidates = OrderedDict(((1, 'Bill Clinton'),
                          (2, 'Bob Dole'),
                          (3, 'Ross Perot'),
                          (4, 'George Bush'),
                          (5, 'Al Gore'),
                          (6, 'John Kerry'),
                          (7, 'Barrack Obama'),
                          (8, 'John McCain'),
                          (9, 'Mitt Romney'),
                          (10, 'Donald Trump'),
                          (11, 'Hillary Clinton')))

states = OrderedDict(((1, 'Texas'),
                      (2, 'California')))

elections = OrderedDict(((1, '1996'),
                         (2, '2000'),
                         (3, '2004'),
                         (4, '2008'),
                         (5, '2012'),
                         (6, '2016')))

election_candidates = {
    1: {'candidates': [1, 2, 3], 'winner': 1},
    2: {'candidates': [4, 5], 'winner': 4},
    3: {'candidates': [4, 6], 'winner': 4},
    4: {'candidates': [7, 8], 'winner': 7},
    5: {'candidates': [7, 9], 'winner': 7},
    6: {'candidates': [10, 11], 'winner': 10},
}

candidate_parties = {
    1: 'd',
    2: 'r',
    3: 'i',
    4: 'r',
    5: 'd',
    6: 'd',
    7: 'd',
    8: 'r',
    9: 'r',
    10: 'r',
    11: 'd',
}

election_candidate_state_votes = {
    # Texas
    (1, 1, 1): 2459683,
    (1, 2, 1): 2736167,
    (1, 3, 1): 378537,
    (2, 4, 1): 3799639,
    (2, 5, 1): 2433746,
    (3, 4, 1): 4526917,
    (3, 6, 1): 2832704,
    (4, 7, 1): 3528633,
    (4, 8, 1): 4479328,
    (5, 7, 1): 4569843,
    (5, 9, 1): 3308124,
    (6, 10, 1): 4685047,
    (6, 11, 1): 387868,

    # California
    (1, 1, 2): 5119835,
    (1, 2, 2): 3828380,
    (1, 3, 2): 697847,
    (2, 4, 2): 4567429,
    (2, 5, 2): 5861203,
    (3, 4, 2): 5509826,
    (3, 6, 2): 6745485,
    (4, 7, 2): 8274473,
    (4, 8, 2): 5011781,
    (5, 7, 2): 7854285,
    (5, 9, 2): 4839958,
    (6, 10, 2): 8753788,
    (6, 11, 2): 4483810,
}

election_candidate_wins = {
    (1, 1): True,
    (1, 2): False,
    (1, 3): False,
    (2, 4): True,
    (2, 5): False,
    (3, 4): True,
    (3, 6): False,
    (4, 7): True,
    (4, 8): False,
    (5, 7): True,
    (5, 9): False,
    (6, 10): True,
    (6, 11): False,
}

df_columns = [fd('timestamp'),
              fd('candidate'), fd('candidate_display'),
              fd('political_party'),
              fd('election'), fd('election_display'),
              fd('state'), fd('state_display'),
              fd('winner'),
              fm('votes'),
              fm('wins')]


def PoliticsRow(timestamp, candidate, candidate_display, political_party, election, election_display, state,
                state_display, winner, votes, wins):
    return (
        timestamp, candidate, candidate_display, political_party, election, election_display, state, state_display,
        winner, votes, wins
    )


records = []
for (election_id, candidate_id, state_id), votes in election_candidate_state_votes.items():
    election_year = elections[election_id]
    winner = election_candidate_wins[(election_id, candidate_id)]

    records.append(PoliticsRow(
          timestamp=datetime(int(election_year), 1, 1),
          candidate=candidate_id, candidate_display=candidates[candidate_id],
          political_party=candidate_parties[candidate_id],
          election=election_id, election_display=elections[election_id],
          state=state_id, state_display=states[state_id],
          winner=winner,
          votes=votes,
          wins=(1 if winner else 0),
    ))

mock_politics_database = pd.DataFrame.from_records(records, columns=df_columns)

single_metric_df = pd.DataFrame(mock_politics_database[[fm('votes')]]
                                .sum()).T

multi_metric_df = pd.DataFrame(mock_politics_database[[fm('votes'), fm('wins')]]
                               .sum()).T

cont_dim_df = mock_politics_database[[fd('timestamp'), fm('votes'), fm('wins')]] \
    .groupby(fd('timestamp')) \
    .sum()

cat_dim_df = mock_politics_database[[fd('political_party'), fm('votes'), fm('wins')]] \
    .groupby(fd('political_party')) \
    .sum()

uni_dim_df = mock_politics_database[[fd('candidate'), fd('candidate_display'), fm('votes'), fm('wins')]] \
    .groupby([fd('candidate'), fd('candidate_display')]) \
    .sum() \
    .reset_index(fd('candidate_display'))

cont_cat_dim_df = mock_politics_database[[fd('timestamp'), fd('political_party'), fm('votes'), fm('wins')]] \
    .groupby([fd('timestamp'), fd('political_party')]) \
    .sum()

cont_uni_dim_df = mock_politics_database[[fd('timestamp'), fd('state'), fd('state_display'), fm('votes'), fm('wins')]] \
    .groupby([fd('timestamp'), fd('state'), fd('state_display')]) \
    .sum() \
    .reset_index(fd('state_display'))

cont_dim_operation_df = cont_dim_df.copy()

operation_key = fm('cumsum(votes)')
cont_dim_operation_df[operation_key] = cont_dim_df[fm('votes')].cumsum()


def ref(data_frame, columns):
    ref_cols = {column: '%s_eoe' % column
                for column in columns}

    ref_df = data_frame \
        .shift(2) \
        .rename(columns=ref_cols)[list(ref_cols.values())]

    return (cont_uni_dim_df
            .copy()
            .join(ref_df)
            .iloc[2:])


def ref_delta(ref_data_frame, columns):
    ref_columns = ['%s_eoe' % column for column in columns]

    delta_data_frame = pd.DataFrame(
          data=ref_data_frame[ref_columns].values - ref_data_frame[columns].values,
          columns=['%s_eoe_delta' % column for column in columns],
          index=ref_data_frame.index
    )
    return ref_data_frame.join(delta_data_frame)


_columns = [fm('votes'), fm('wins')]
cont_uni_dim_ref_df = ref(cont_uni_dim_df, _columns)
cont_uni_dim_ref_delta_df = ref_delta(cont_uni_dim_ref_df, _columns)


def totals(data_frame, dimensions, columns):
    """
    Computes the totals across a dimension and adds the total as an extra row.
    """

    def _totals(df):
        if isinstance(df, pd.Series):
            return df.sum()

        return pd.DataFrame(
              [df.sum()],
              columns=columns,
              index=pd.Index([None],
                             name=df.index.names[-1]))

    totals_df = None
    for i in range(-1, -1 - len(dimensions), -1):
        groupby_levels = data_frame.index.names[:i]

        if groupby_levels:
            level_totals_df = data_frame[columns].groupby(level=groupby_levels).apply(_totals)
        else:
            level_totals_df = pd.DataFrame([data_frame[columns].apply(_totals)],
                                           columns=columns,
                                           index=pd.MultiIndex.from_tuples([[None] * len(data_frame.index.levels)],
                                                                           names=data_frame.index.names))

        totals_df = totals_df.append(level_totals_df) \
            if totals_df is not None \
            else level_totals_df

    return data_frame.append(totals_df).sort_index()


# Convert all index values to string
for l in list(locals().values()):
    if not isinstance(l, pd.DataFrame):
        continue

    if hasattr(l.index, 'levels'):
        l.index = pd.MultiIndex(levels=[level.astype('str')
                                        if not isinstance(level, (pd.DatetimeIndex, pd.RangeIndex))
                                        else level
                                        for level in l.index.levels],
                                labels=l.index.labels)
    elif not isinstance(l.index, (pd.DatetimeIndex, pd.RangeIndex)):
        l.index = l.index.astype('str')

cont_cat_dim_totals_df = totals(cont_cat_dim_df, [fd('political_party')], _columns)
cont_uni_dim_totals_df = totals(cont_uni_dim_df, [fd('state')], _columns)
cont_uni_dim_all_totals_df = totals(cont_uni_dim_df, [fd('timestamp'), fd('state')], _columns)

ElectionOverElection = ReferenceType('eoe', 'EoE', 'year', 4)

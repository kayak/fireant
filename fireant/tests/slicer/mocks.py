from collections import (
    OrderedDict,
    namedtuple,
)
from unittest.mock import Mock

import pandas as pd
from datetime import date

from fireant import *
from fireant import VerticaDatabase
from pypika import (
    JoinType,
    Table,
    functions as fn,
)


class TestDatabase(VerticaDatabase):
    # Vertica client that uses the vertica_python driver.

    connect = Mock()


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
          Join(table=state_table,
               criterion=district_table.state_id == state_table.id),
          Join(table=voters_table,
               criterion=district_table.id == voters_table.district_id),
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

columns = ['timestamp',
           'candidate', 'candidate_display',
           'political_party',
           'election', 'election_display',
           'state', 'state_display',
           'winner',
           'votes',
           'wins']
PoliticsRow = namedtuple('PoliticsRow', columns)

records = []
for (election_id, candidate_id, state_id), votes in election_candidate_state_votes.items():
    election_year = elections[election_id]
    winner = election_candidate_wins[(election_id, candidate_id)]
    records.append(PoliticsRow(
          timestamp=date(int(election_year), 1, 1),
          candidate=candidate_id, candidate_display=candidates[candidate_id],
          political_party=candidate_parties[candidate_id],
          election=election_id, election_display=elections[election_id],
          state=state_id, state_display=states[state_id],
          winner=winner,
          votes=votes,
          wins=(1 if winner else 0),
    ))

mock_politics_database = pd.DataFrame.from_records(records, columns=columns)

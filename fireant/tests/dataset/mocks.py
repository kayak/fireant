from collections import OrderedDict
from datetime import datetime
from unittest.mock import MagicMock, Mock

import pandas as pd
from pypika import Case, JoinType, Table, functions as fn

from fireant import *
from fireant.dataset.annotations import Annotation
from fireant.dataset.references import ReferenceType
from fireant.dataset.totals import get_totals_marker_for_dtype
from fireant.utils import alias_selector as f


class TestDatabaseMixin:
    # Vertica client that uses the vertica_python driver.

    connect = Mock()
    get_column_definitions = MagicMock(return_value=[])

    def __eq__(self, other):
        return isinstance(other, TestDatabaseMixin)


class TestVerticaDatabase(TestDatabaseMixin, VerticaDatabase):
    pass


class TestMySQLDatabase(TestDatabaseMixin, MySQLDatabase):
    pass


test_database = TestVerticaDatabase()
politicians_table = Table("politician", schema="politics")
politicians_spend_table = Table("politician_spend", schema="politics")
politicians_staff_table = Table("politician_staff", schema="politics")
politicians_hint_table = Table("hints", schema="politics")
politicians_annotation_table = Table("annotations", schema="politics")
voters_table = Table("voter", schema="politics")
state_table = Table("state", schema="locations")
district_table = Table("district", schema="locations")
deep_join_table = Table("deep", schema="test")

mock_dataset = DataSet(
    table=politicians_table,
    database=test_database,
    fields=[
        Field(
            "timestamp",
            label="Timestamp",
            definition=politicians_table.timestamp,
            data_type=DataType.date,
        ),
        Field(
            "timestamp2",
            label="Timestamp 2",
            definition=politicians_table.timestamp2,
            data_type=DataType.date,
        ),
        Field(
            "join_timestamp",
            label="Join Timestamp",
            definition=voters_table.timestamp,
            data_type=DataType.date,
        ),
        Field(
            "political_party",
            label="Party",
            definition=politicians_table.political_party,
            data_type=DataType.text,
            hyperlink_template="http://example.com/{political_party}",
        ),
        Field(
            "candidate-id",
            label="Candidate ID",
            definition=politicians_table.candidate_id,
            data_type=DataType.number,
        ),
        Field(
            "candidate-name",
            label="Candidate Name",
            definition=politicians_table.candidate_name,
            data_type=DataType.text,
            hyperlink_template="http://example.com/{political_party}/{candidate-name}",
        ),
        Field(
            "election-id",
            label="Election ID",
            definition=politicians_table.election_id,
            data_type=DataType.number,
        ),
        Field(
            "election-year",
            label="Election Year",
            definition=politicians_table.election_year,
            data_type=DataType.number,
        ),
        Field(
            "district-id",
            label="District ID",
            definition=politicians_table.district_id,
            data_type=DataType.number,
        ),
        Field(
            "district-name",
            label="District Name",
            definition=district_table.district_name,
            data_type=DataType.text,
        ),
        Field(
            "state",
            label="State",
            definition=state_table.state_name,
            data_type=DataType.text,
        ),
        Field(
            "winner",
            label="Winner",
            definition=politicians_table.is_winner,
            data_type=DataType.boolean,
        ),
        Field("deepjoin", definition=deep_join_table.id, data_type=DataType.number),
        Field(
            "votes",
            label="Votes",
            definition=fn.Sum(politicians_table.votes),
            data_type=DataType.number,
            thousands=",",
        ),
        Field("wins", label="Wins", definition=fn.Sum(politicians_table.is_winner)),
        Field("voters", label="Voters", definition=fn.Count(voters_table.id)),
        Field(
            "turnout",
            label="Turnout",
            definition=fn.Sum(politicians_table.votes) / fn.Count(voters_table.id),
            suffix="%",
            precision=2,
        ),
        Field(
            "wins_with_style",
            label="Wins",
            definition=fn.Sum(politicians_table.is_winner),
            prefix="$",
            thousands="_",
            precision=0,
        ),
    ],
    joins=[
        Join(
            table=district_table,
            criterion=politicians_table.district_id == district_table.id,
            join_type=JoinType.outer,
        ),
        Join(
            table=district_table,
            criterion=politicians_table.district_id == district_table.id,
            join_type=JoinType.outer,
        ),
        Join(table=state_table, criterion=district_table.state_id == state_table.id),
        Join(
            table=voters_table,
            criterion=politicians_table.id == voters_table.politician_id,
        ),
        Join(table=deep_join_table, criterion=deep_join_table.id == state_table.ref_id),
    ],
)

mock_spend_dataset = DataSet(
    table=politicians_spend_table,
    database=test_database,
    fields=[
        Field(
            "timestamp",
            label="Timestamp",
            definition=politicians_spend_table.timestamp,
            data_type=DataType.date,
        ),
        Field(
            "candidate-id",
            label="Candidate ID",
            definition=politicians_spend_table.candidate_id,
            data_type=DataType.number,
        ),
        Field(
            "candidate-spend",
            label="Candidate Spend",
            definition=fn.Sum(politicians_spend_table.candidate_spend),
            data_type=DataType.number,
        ),
        Field(
            "election-year",
            label="Election Year",
            definition=politicians_spend_table.election_year,
            data_type=DataType.number,
        ),
        Field(
            "state",
            label="State",
            definition=politicians_spend_table.state_name,
            data_type=DataType.text,
        ),
        Field(
            "special",
            label="special",
            definition=politicians_spend_table.special,
            data_type=DataType.text,
        ),
    ],
).extra_fields(
    Field(
        "id-of-the-district",
        label="District Id",
        definition=politicians_spend_table.id_of_the_district,
        data_type=DataType.number,
    )
)

mock_staff_dataset = DataSet(
    table=politicians_staff_table,
    database=test_database,
    fields=[
        Field(
            "candidate-id",
            label="Candidate ID",
            definition=politicians_staff_table.candidate_id,
            data_type=DataType.number,
        ),
        Field(
            "num_staff",
            label="Num. Staff",
            definition=fn.Count(politicians_staff_table.staff_id),
            data_type=DataType.number,
        ),
    ],
)

mock_dataset_blender = (
    mock_dataset.blend(mock_spend_dataset)
    .on_dimensions()
    .extra_fields(
        Field(
            "candidate-spend-per-voters",
            label="Average Candidate Spend per Voters",
            definition=(mock_spend_dataset.fields["candidate-spend"] / mock_dataset.fields["voters"]),
            data_type=DataType.number,
        ),
        Field(
            "candidate-spend-per-wins",
            label="Average Candidate Spend per Wins",
            definition=(mock_spend_dataset.fields["candidate-spend"] / mock_dataset.fields["wins"]),
            data_type=DataType.number,
        ),
    )
)

mock_case = (
    Case()
    .when(politicians_table.political_party == "Democrat", "Democrat")
    .when(politicians_table.candidate_name == "Bill Clinton", "Bill Clinton")
    .else_("No One")
)

mock_date_annotation_dataset = DataSet(
    table=politicians_table,
    database=test_database,
    fields=[
        Field(
            "timestamp",
            label="Timestamp",
            definition=politicians_table.timestamp,
            data_type=DataType.date,
        ),
        Field(
            "political_party",
            label="Party",
            definition=politicians_table.political_party,
            data_type=DataType.text,
        ),
        Field(
            "votes",
            label="Votes",
            definition=fn.Sum(politicians_table.votes),
            data_type=DataType.number,
            thousands=",",
        ),
    ],
    annotation=Annotation(
        table=politicians_annotation_table,
        field=Field(
            "district-name",
            definition=politicians_annotation_table.district_name,
            data_type=DataType.text,
        ),
        alignment_field=Field(
            "timestamp2",
            label="Timestamp 2",
            definition=politicians_annotation_table.timestamp2,
            data_type=DataType.date,
        ),
        dataset_alignment_field_alias="timestamp",
    ),
)

mock_category_annotation_dataset = DataSet(
    table=politicians_table,
    database=test_database,
    fields=[
        Field(
            "political_party",
            label="Party",
            definition=politicians_table.political_party,
            data_type=DataType.text,
        ),
        Field(
            "votes",
            label="Votes",
            definition=fn.Sum(politicians_table.votes),
            data_type=DataType.number,
            thousands=",",
        ),
    ],
    annotation=Annotation(
        table=politicians_annotation_table,
        field=Field(
            alias="district-name",
            definition=politicians_annotation_table.district_name,
            data_type=DataType.text,
        ),
        alignment_field=Field(
            "political_party",
            label="Party",
            definition=politicians_annotation_table.political_party,
            data_type=DataType.text,
        ),
        dataset_alignment_field_alias="political_party",
    ),
)

mock_hint_dataset = DataSet(
    table=politicians_table,
    database=test_database,
    joins=[
        Join(
            table=district_table,
            criterion=politicians_table.district_id == district_table.id,
            join_type=JoinType.outer,
        ),
        Join(table=state_table, criterion=politicians_table.state_id == state_table.id),
    ],
    fields=[
        Field(
            "political_party",
            label="Party",
            hint_table=politicians_hint_table,
            definition=politicians_table.political_party,
            data_type=DataType.text,
        ),
        Field(
            "candidate_name_display",
            label="Candidate Name Display",
            definition=politicians_table.candidate_name_display,
            data_type=DataType.text,
        ),
        Field(
            "candidate_name",
            label="Candidate Name",
            hint_table=politicians_hint_table,
            definition=politicians_table.candidate_name,
            data_type=DataType.text,
        ),
        Field(
            "election-year",
            label="Election Year",
            definition=politicians_table.election_year,
            data_type=DataType.number,
        ),
        Field(
            "district-name",
            label="District Name",
            hint_table=politicians_hint_table,
            definition=district_table.district_name,
            data_type=DataType.text,
        ),
        Field(
            "political_party_case",
            label="Party",
            definition=mock_case,
            data_type=DataType.text,
        ),
        Field(
            "state",
            label="State",
            definition=state_table.state_name,
            data_type=DataType.text,
        ),
    ],
)

political_parties = OrderedDict(
    (
        ("d", "Democrat"),
        ("r", "Republican"),
        ("i", "Independent"),
        ("l", "Libertarian"),
        ("g", "Green"),
        ("c", "Constitution"),
    )
)

candidates = OrderedDict(
    (
        (1, "Bill Clinton"),
        (2, "Bob Dole"),
        (3, "Ross Perot"),
        (4, "George Bush"),
        (5, "Al Gore"),
        (6, "John Kerry"),
        (7, "Barrack Obama"),
        (8, "John McCain"),
        (9, "Mitt Romney"),
        (10, "Donald Trump"),
        (11, "Hillary Clinton"),
    )
)

states = OrderedDict(((1, "Texas"), (2, "California")))

elections = OrderedDict(((1, "1996"), (2, "2000"), (3, "2004"), (4, "2008"), (5, "2012"), (6, "2016")))

election_candidates = {
    1: {"candidates": [1, 2, 3], "winner": 1},
    2: {"candidates": [4, 5], "winner": 4},
    3: {"candidates": [4, 6], "winner": 4},
    4: {"candidates": [7, 8], "winner": 7},
    5: {"candidates": [7, 9], "winner": 7},
    6: {"candidates": [10, 11], "winner": 10},
}

candidate_parties = {
    1: "d",
    2: "r",
    3: "i",
    4: "r",
    5: "d",
    6: "d",
    7: "d",
    8: "r",
    9: "r",
    10: "r",
    11: "d",
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

df_columns = [
    f("timestamp"),
    f("candidate-id"),
    f("candidate-name"),
    f("political_party"),
    f("election-id"),
    f("election-year"),
    f("state"),
    f("winner"),
    f("votes"),
    f("wins"),
]


def PoliticsRow(
    timestamp,
    candidate_id,
    candidate_name,
    political_party,
    election_year,
    state,
    winner,
    votes,
    wins,
):
    return (
        timestamp,
        candidate_id,
        candidate_name,
        political_party,
        election_year,
        state,
        state,
        winner,
        votes,
        wins,
    )


records = []
for (
    (election_id, candidate_id, state_id),
    votes,
) in election_candidate_state_votes.items():
    election_year = elections[election_id]
    winner = election_candidate_wins[(election_id, candidate_id)]

    records.append(
        PoliticsRow(
            timestamp=datetime(int(election_year), 1, 1),
            candidate_id=candidate_id,
            candidate_name=candidates[candidate_id],
            political_party=political_parties[candidate_parties[candidate_id]],
            election_year=elections[election_id],
            state=states[state_id],
            winner=winner,
            votes=votes,
            wins=(1 if winner else 0),
        )
    )

mock_politics_database = pd.DataFrame.from_records(records, columns=df_columns)
mock_politics_database[f("wins_with_style")] = mock_politics_database[f("wins")]
mock_politics_database[f("turnout")] = 25 * mock_politics_database[f("wins")]

dimx0_metricx1_df = pd.DataFrame(mock_politics_database[[f("votes")]].sum()).T

metrics = [f("votes"), f("wins"), f("wins_with_style"), f("turnout")]
dimx0_metricx2_df = pd.DataFrame(mock_politics_database[metrics].sum()).T

dimx1_date_df = mock_politics_database[[f("timestamp")] + metrics].groupby(f("timestamp")).sum()

dimx1_date_meticx1_votes_df = mock_politics_database[[f("timestamp")] + [f("votes")]].groupby(f("timestamp")).sum()

no_index_df = pd.DataFrame(dimx1_date_df.sum()).T

dimx1_str_df = mock_politics_database[[f("political_party")] + metrics].groupby(f("political_party")).sum()

dimx2_date_index_str_df = mock_politics_database[[f("timestamp"), f("candidate-name")]].set_index(f("timestamp"))

dimx2_category_index_str_df = mock_politics_database[[f("political_party"), f("candidate-name")]].set_index(
    f("political_party")
)

dimx1_none_df = pd.DataFrame(mock_politics_database[metrics].sum()).T

dimx1_num_df = mock_politics_database[[f("candidate-id")] + metrics].groupby(f("candidate-id")).sum()

dimx2_str_num_df = (
    mock_politics_database[[f("political_party"), f("candidate-id")] + metrics]
    .groupby([f("political_party"), f("candidate-id")])
    .sum()
)

dimx2_str_str_df = (
    mock_politics_database[[f("political_party"), f("candidate-name")] + metrics]
    .groupby([f("political_party"), f("candidate-name")])
    .sum()
)

dimx2_date_str_df = (
    mock_politics_database[[f("timestamp"), f("political_party")] + metrics]
    .groupby([f("timestamp"), f("political_party")])
    .sum()
)

dimx2_date_bool_df = (
    mock_politics_database[[f("timestamp"), f("winner"), f("votes")]].groupby([f("timestamp"), f("winner")]).sum()
)

dimx2_date_num_df = (
    mock_politics_database[[f("timestamp"), f("candidate-id")] + metrics]
    .groupby([f("timestamp"), f("candidate-id")])
    .sum()
)

dimx3_date_str_str_df = (
    mock_politics_database[[f("timestamp"), f("political_party"), f("state")] + metrics]
    .groupby([f("timestamp"), f("political_party"), f("state")])
    .sum()
)

dimx1_date_operation_df = dimx1_date_df.copy()

operation_key = f("cumsum(votes)")
dimx1_date_operation_df[operation_key] = dimx1_date_df[f("votes")].cumsum()


def split(list, i):
    return list[:i], list[i:]


def ref(data_frame, columns):
    ref_cols = {column: "%s_eoe" % column for column in columns}

    ref_df = data_frame.shift(2).rename(columns=ref_cols)[list(ref_cols.values())]

    return dimx2_date_str_df.copy().join(ref_df).iloc[2:]


def ref_delta(ref_data_frame, columns):
    ref_columns = ["%s_eoe" % column for column in columns]

    delta_data_frame = pd.DataFrame(
        data=ref_data_frame[ref_columns].values - ref_data_frame[columns].values,
        columns=["%s_eoe_delta" % column for column in columns],
        index=ref_data_frame.index,
    )
    return ref_data_frame.join(delta_data_frame)


_columns = metrics
dimx2_date_str_ref_df = ref(dimx2_date_str_df, _columns)
dimx2_date_str_ref_delta_df = ref_delta(dimx2_date_str_ref_df, _columns)


def totals(data_frame, dimensions, columns):
    """
    Computes the totals across a dimension and adds the total as an extra row.
    """
    if not isinstance(data_frame.index, pd.MultiIndex):
        totals_marker = get_totals_marker_for_dtype(data_frame.index.dtype)
        totals_df = pd.DataFrame(
            [data_frame.sum()],
            index=pd.Index([totals_marker], name=data_frame.index.name),
        )

        return data_frame.append(totals_df)

    def _totals(df):
        if isinstance(df, pd.Series):
            return df.sum()

        totals_index_value = get_totals_marker_for_dtype(df.index.levels[-1].dtype)

        return pd.DataFrame(
            [df.sum()],
            columns=columns,
            index=pd.Index([totals_index_value], name=df.index.names[-1]),
        )

    totals_df = None
    for i in range(-1, -1 - len(dimensions), -1):
        groupby_levels = data_frame.index.names[:i]

        if groupby_levels:
            level_totals_df = data_frame[columns].groupby(level=groupby_levels).apply(_totals)

            missing_dims = set(data_frame.index.names) - set(level_totals_df.index.names)
            if missing_dims:
                for dim in missing_dims:
                    dtype = data_frame.index.levels[data_frame.index.names.index(dim)].dtype
                    level_totals_df[dim] = get_totals_marker_for_dtype(dtype)
                    level_totals_df.set_index(dim, append=True, inplace=True)

                level_totals_df = level_totals_df.reorder_levels(data_frame.index.names)

        else:
            totals_index_values = [get_totals_marker_for_dtype(level.dtype) for level in data_frame.index.levels]
            level_totals_df = pd.DataFrame(
                [data_frame[columns].apply(_totals)],
                columns=columns,
                index=pd.MultiIndex.from_tuples([totals_index_values], names=data_frame.index.names),
            )

        totals_df = totals_df.append(level_totals_df) if totals_df is not None else level_totals_df

    return data_frame.append(totals_df).sort_index()


# Convert all index values to string
# for l in list(locals().values()):
#     if not isinstance(l, pd.DataFrame):
#         continue
#
#     if hasattr(l.index, 'levels'):
#         l.index = pd.MultiIndex(levels=[level.astype('str')
#                                         if not isinstance(level, (pd.DatetimeIndex, pd.RangeIndex))
#                                         else level
#                                         for level in l.index.levels],
#                                 labels=l.index.labels)
#     elif not isinstance(l.index, (pd.DatetimeIndex, pd.RangeIndex)):
#         l.index = l.index.astype('str')

dimx1_str_totals_df = totals(dimx1_str_df, [f("political_party")], _columns)
dimx2_date_str_totals_df = totals(dimx2_date_str_df, [f("political_party")], _columns)
dimx2_date_str_totalsx2_df = totals(dimx2_date_str_df, [f("timestamp"), f("political_party")], _columns)
dimx3_date_str_str_totalsx3_df = totals(
    dimx3_date_str_str_df, [f("timestamp"), f("political_party"), f("state")], _columns
)

ElectionOverElection = ReferenceType("eoe", "EoE", "year", 4)

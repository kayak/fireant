from fireant import (
    Field,
)
from fireant.dataset.klass import (
    DataSet,
)
from fireant.queries import (
    DimensionChoicesQueryBuilder,
    DimensionLatestQueryBuilder,
)
from fireant.queries.builder.dataset_blender_query_builder import DataSetBlenderQueryBuilder
from fireant.utils import immutable


class DataSetBlender(object):
    """
    WRITEME
    """

    def __init__(self, primary_dataset):
        """
        Constructor for a data blended slicer.  Contains all the fields to initialize the slicer.

        :param primary_dataset: (Required)
            The primary dataset, which table will be used as part of the FROM expression.
        """
        self.primary_dataset = primary_dataset
        self.table = primary_dataset.table
        self.secondary_datasets = []
        self.fields = DataSet.Fields()

        self.field_mapping = {}

        primary_dataset_field_mapping = {}

        for primary_field in self.primary_dataset.fields:
            primary_dataset_field_mapping[primary_field] = primary_field

        self.field_mapping[primary_dataset.table._table_name] = primary_dataset_field_mapping

        for primary_field in self.primary_dataset.fields:
            if primary_field.alias not in self.fields:
                self.fields.append(primary_field)

        # add query builder entry points
        self.query = DataSetBlenderQueryBuilder(self)
        self.latest = DimensionLatestQueryBuilder(self.primary_dataset)

    def __eq__(self, other):
        return isinstance(other, DataSetBlender) \
               and self.fields == other.fields

    def __repr__(self):
        return 'BlendedSlicer(fields=[{}])' \
            .format(','.join([repr(f)
                              for f in self.fields]))

    def __hash__(self):
        return hash((
            self.table, self.primary_dataset.database, tuple(self.secondary_datasets), self.joins, self.fields,
        ))

    @immutable
    def join(self, secondary_dataset, field_mapping=None):
        """
        Adds a join when building a slicer query. The join type is left outer by default.

        :param secondary_dataset: (Required)
            The secondary dataset, which table will be used as part of the one or more JOIN expressions.
        :param field_mapping: (Optional)
            A dictionary with primary dataset's fields as keys and the provided secondary dataset's fields as values.
            If not provided, fields with the same name will be matched by default.

        :return:
            A copy of the query with the join added.
        """
        if field_mapping is None:
            field_mapping = {}

            for primary_field in self.primary_dataset.fields:
                for secondary_field in secondary_dataset.fields:
                    if primary_field.alias == secondary_field.alias:
                        field_mapping[primary_field] = secondary_field

        self.field_mapping[secondary_dataset.table._table_name] = field_mapping

        for secondary_field in secondary_dataset.fields:
            if secondary_field.alias not in self.fields:
                self.fields.append(secondary_field)

        self.secondary_datasets.append(secondary_dataset)

    @immutable
    def field(self, *args, **kwargs):
        """
        Adds a field when building a slicer query. Fields are similar to a column in a database query result set.

        :return:
            A copy of the query with the field added.
        """
        field = Field(self, *args, **kwargs)

        if not field.definition.is_aggregate:
            field.choices = DimensionChoicesQueryBuilder(self.primary_dataset, field)

        self.fields.append(
            field
        )

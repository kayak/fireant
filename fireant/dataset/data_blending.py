from fireant.dataset.fields import Field
from fireant.dataset.klass import DataSet
from fireant.queries.builder import DataSetBlenderQueryBuilder
from fireant.utils import immutable


class DataSetBlender:
    """
    The DataSetBlender class is the DataSet equivalent for implementing data blending, across distinct DataSet
    instances.
    """

    def __init__(self, primary_dataset, secondary_dataset, field_map):
        """
        Constructor for a blended dataset.  Contains all the fields to initialize the dataset.

        :param primary_dataset: (Required)
            The primary dataset, which table will be used as part of the FROM expression. This can be either a `DataSet`
            or another `DataSetBlender`, which means multiple DataSet instances can be blended by chaining together
            blenders.
        :param secondary_dataset: (Required)
            The dataset being blended. This should be a `DataSet` instance. (It might actually work with an instance of
            `DataSetBlender` as well, though.)
        :param field_map:
            A dict mapping up fields from the primary to the secondary dataset. This tells the Blender which fields
            can be used as dimensions in the Blender queries.
        """
        self.primary_dataset = primary_dataset
        self.secondary_dataset = secondary_dataset
        self.field_map = field_map

        # Wrap all dataset fields with another field on top so that:
        #   1. DataSetBlender doesn't share a reference to a field with a DataSet
        #   2. When complex fields are added, the `definition` attribute will always have at least one field within
        #      its object graph
        all_fields = [*secondary_dataset.fields, *primary_dataset.fields]
        self.fields = DataSet.Fields(
            [
                Field(
                    alias=field.alias,
                    definition=field,
                    data_type=field.data_type,
                    label=field.label,
                    hint_table=field.hint_table,
                    prefix=field.prefix,
                    suffix=field.suffix,
                    thousands=field.thousands,
                    precision=field.precision,
                    hyperlink_template=field.hyperlink_template,
                )
                for field in all_fields
            ]
        )

        # add query builder entry points
        self.query = DataSetBlenderQueryBuilder(self)
        self.latest = self.primary_dataset.latest

    def __eq__(self, other):
        return isinstance(other, DataSetBlender) and self.fields == other.fields

    def __repr__(self):
        return "BlendedDatSet(fields=[{}])".format(
            ",".join([repr(f) for f in self.fields])
        )

    def __hash__(self):
        return hash((self.primary_dataset, self.secondary_dataset, self.fields))

    @property
    def table(self):
        return None

    @immutable
    def extra_fields(self, *fields):
        for field in fields:
            self.fields.append(field)

    def blend(self, other):
        """
        Returns a Data Set blender which enables to execute queries on multiple data sets and combine them.
        """
        return DataSetBlenderBuilder(self, other)


class DataSetBlenderBuilder:
    def __init__(self, primary, secondary):
        self.primary_dataset = primary
        self.secondary_dataset = secondary

    def on(self, field_map):
        return DataSetBlender(self.primary_dataset, self.secondary_dataset, field_map)

    def on_dimensions(self):
        field_map = {}

        for secondary_ds_field in self.secondary_dataset.fields:
            is_aggregate_field = secondary_ds_field.is_aggregate
            matches_alias_in_primary_dataset = (
                secondary_ds_field.alias in self.primary_dataset.fields
            )
            if is_aggregate_field or not matches_alias_in_primary_dataset:
                continue

            primary_ds_field = self.primary_dataset.fields[secondary_ds_field.alias]
            field_map[primary_ds_field] = secondary_ds_field

        return self.on(field_map)

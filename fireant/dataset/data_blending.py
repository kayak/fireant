from fireant.dataset.fields import Field
from fireant.dataset.klass import DataSet
from fireant.queries.builder import (
    DataSetBlenderQueryBuilder,
    DimensionChoicesQueryBuilder,
)
from fireant.utils import (
    deepcopy,
    immutable,
    ordered_distinct_list_by_attr,
)


def _wrap_dataset_fields(dataset):
    if isinstance(dataset, DataSetBlender):
        return dataset.fields

    wrapped_fields = []
    for field in dataset.fields:
        wrapped_field = _wrap_field(dataset, field)

        wrapped_fields.append(wrapped_field)

    return wrapped_fields


def _wrap_field(dataset, field):
    wrapped_field = Field(
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

    if not field.definition.is_aggregate:
        wrapped_field.choices = DimensionChoicesBlenderQueryBuilder(dataset, field)

    return wrapped_field


class DataSetBlender:
    """
    The DataSetBlender class is the DataSet equivalent for implementing data blending, across distinct DataSet
    instances.
    """

    def __init__(self, primary_dataset, secondary_dataset, dimension_map):
        """
        Constructor for a blended dataset.  Contains all the fields to initialize the dataset.

        :param primary_dataset: (Required)
            The primary dataset, which table will be used as part of the FROM expression. This can be either a `DataSet`
            or another `DataSetBlender`, which means multiple DataSet instances can be blended by chaining together
            blenders.
        :param secondary_dataset: (Required)
            The dataset being blended. This should be a `DataSet` instance. (It might actually work with an instance of
            `DataSetBlender` as well, though.)
        :param dimension_map:
            A dict mapping up fields from the primary to the secondary dataset. This tells the Blender which fields
            can be used as dimensions in the Blender queries.
        """
        self.primary_dataset = primary_dataset
        self.secondary_dataset = secondary_dataset
        self.dimension_map = dimension_map

        # Wrap all dataset fields with another field on top so that:
        #   1. DataSetBlender doesn't share a reference to a field with a DataSet (__hash__ is used to find out which
        #      dataset the field is in - see the Field class' __hash__ method for more details)
        #   2. When complex fields are added, the `definition` attribute will always have at least one field within
        #      its object graph
        self.fields = DataSet.Fields(
            ordered_distinct_list_by_attr(
                [
                    *_wrap_dataset_fields(primary_dataset),
                    *_wrap_dataset_fields(secondary_dataset)
                ],
            )
        )

        # add query builder entry points
        self.query = DataSetBlenderQueryBuilder(self)
        self.latest = self.primary_dataset.latest
        self.annotation = None

    @property
    def return_additional_metadata(self) -> bool:
        # When using data blending, datasets are nested inside DataSetBlender objects. Additionally,
        # the primary_dataset can be a combination of datasets depending on how many datasets are being blended.
        # This helper property walks the tree to return the return_additional_metadata value from the original
        # primary dataset.
        dataset = self.primary_dataset
        while not isinstance(dataset, DataSet):
            dataset = dataset.primary_dataset

        return dataset.return_additional_metadata

    def __eq__(self, other):
        return isinstance(other, DataSetBlender) and self.fields == other.fields

    def __repr__(self):
        return "BlendedDataSet(fields=[{}])".format(
            ",".join([repr(f) for f in self.fields])
        )

    def __hash__(self):
        return hash((self.primary_dataset, self.secondary_dataset, self.fields))

    def __deepcopy__(self, memodict={}):
        for field in self.dimension_map.values():
            memodict[id(field)] = field
        return deepcopy(self, memodict)

    @property
    def table(self):
        return None

    @property
    def database(self):
        return self.primary_dataset.database

    @immutable
    def extra_fields(self, *fields):
        for field in fields:
            self.fields.add(field)

    def blend(self, other):
        """
        Returns a Data Set blender which enables to execute queries on multiple data sets and combine them.
        """
        return DataSetBlenderBuilder(self, other)


class DataSetBlenderBuilder:
    def __init__(self, primary, secondary):
        self.primary_dataset = primary
        self.secondary_dataset = secondary

    def on(self, dimension_map):
        return DataSetBlender(
            self.primary_dataset, self.secondary_dataset, dimension_map
        )

    def on_dimensions(self):
        dimension_map = {}

        for secondary_ds_field in self.secondary_dataset.fields:
            is_aggregate_field = secondary_ds_field.is_aggregate
            matches_alias_in_primary_dataset = (
                secondary_ds_field.alias in self.primary_dataset.fields
            )
            if is_aggregate_field or not matches_alias_in_primary_dataset:
                continue

            primary_ds_field = self.primary_dataset.fields[secondary_ds_field.alias]
            dimension_map[primary_ds_field] = secondary_ds_field

        return self.on(dimension_map)


class DimensionChoicesBlenderQueryBuilder(DimensionChoicesQueryBuilder):
    def filter(self, *filters, **kwargs):
        filters = [
            fltr.for_(fltr.field.definition)
            for fltr in filters
            if fltr.field.definition in self.dataset.fields
        ]
        return super().filter(*filters, **kwargs)

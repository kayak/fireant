from fireant.dataset.fields import Field
from fireant.utils import (
    alias_for_alias_selector,
    immutable,
)
from .query_builder import (
    QueryBuilder,
    QueryException,
)
from ..sql_transformer import make_latest_query


class DimensionLatestQueryBuilder(QueryBuilder):
    def __init__(self, dataset):
        super(DimensionLatestQueryBuilder, self).__init__(dataset, dataset.table)

    @immutable
    def __call__(self, dimension: Field, *dimensions: Field):
        self._dimensions += [dimension] + list(dimensions)

    @property
    def sql(self):
        """
        Serializes this query builder as a set of SQL queries.  This method will always return a list of one
        query since only one query is required to retrieve dimension choices.

        This function only handles dimensions (select+group by) and filtering (where/having), which is everything
        needed for the query to fetch choices for dimensions.

        The dataset query extends this with metrics, references, and totals.
        """
        if not self.dimensions:
            raise QueryException(
                "Must select at least one dimension to query latest values"
            )

        query = make_latest_query(
            database=self.dataset.database,
            base_table=self.table,
            joins=self.dataset.joins,
            dimensions=self.dimensions,
        )
        return [query]

    def fetch(self, hint=None):
        data = super().fetch(hint=hint).reset_index().iloc[0]
        # Remove the row index as the name and trim the special dimension key characters from the dimension key
        data.name = None
        data.index = [alias_for_alias_selector(alias) for alias in data.index]
        return data

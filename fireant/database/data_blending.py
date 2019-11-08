from .type_engine import make_columns


class DataBlender:
    def __init__(self, primary_db, primary_type_engine, secondary_db, secondary_type_engine):
        self.primary_db = primary_db
        self.primary_type_engine = primary_type_engine
        self.secondary_db = secondary_db
        self.secondary_type_engine = secondary_type_engine

    def derive_columns_from_secondary(self, secondary_schema, secondary_table):
        """
        Derives columns from a table of a secondary data source.

        :param secondary_schema: The schema of a table in the secondary data source.
        :param secondary_table: The table of a secondary data source.
        :return: The columns of the secondary tables as a list of Column instances.
        """
        secondary_columns = self.secondary_db.get_column_definitions(secondary_schema, secondary_table)
        return make_columns(secondary_columns, self.secondary_type_engine)

    def transform_columns_to_primary(self, columns):
        """
        Transforms a list of columns to database specific Pypika-Columns using the primary database type engine.

        :param columns: The list of Column instances.
        :return: A list of Pypika-Columns with the transformed database types.
        """
        return [column.as_pypika_column(self.primary_type_engine) for column in columns]

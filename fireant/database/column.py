from pypika.queries import Column as PypikaColumn


class Column:
    """
    Represents an abstract database column.
    """
    def __init__(self, column_name, column_type):
        self.name = column_name
        self.type = column_type

    def __eq__(self, other):
        return str(self) == str(other)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return '{name} {type}'.format(
              name=self.name,
              type=str(self.type),
        )

    def to_database_column(self, type_engine):
        """
        Returns a database specific column representation. The provided type engine is used to match the columns type
        to its database specific representation.

        :param type_engine: The type engine for converting the column's type.
        :return: A database specific representation of the column.
        """
        return '{name} {type}'.format(
              name=self.name,
              type=type_engine.from_ansi(self.type),
        )

    def to_pypika_column(self, type_engine):
        """
        Returns the column as a Pypika Column instance.

        :param type_engine: The type engine for converting the column's type.
        :return: A Pypika Column instance.
        """
        return PypikaColumn(
              column_name=self.name,
              column_type=type_engine.from_ansi(self.type),
        )


def make_columns(db_columns, db_type_engine):
    """
    Creates a list of Column instances with ANSI types from db columns. The function expects
    that the provided type engine is able match the provided db column types.

    :param db_columns: A list of column definitions.
    :param db_type_engine: A type engine to convert column types to ansi types.
    :return: A list of Column instances with ANSI types.
    """
    columns = []
    for column_definition in db_columns:
        column_name = column_definition[0]
        column_type = db_type_engine.to_ansi(column_definition[1])
        columns.append(Column(column_name, column_type))

    return columns


class ColumnsTransformer:
    @staticmethod
    def database_to_ansi(db_columns, db):
        """
        Transforms database specific columns to a list of Column instances.

        :param db_columns: The list of database columns.
        :param db: The database instance.
        :return: The columns of the secondary tables as a list of Column instances.
        """
        return make_columns(db_columns, db.type_engine)

    @staticmethod
    def ansi_to_database(columns, db):
        """
        Transforms a list of Column instances to database specific Pypika-Columns.

        :param columns: The list of Column instances.
        :param db: The database instance.
        :return: A list of Pypika-Columns with the transformed database types.
        """
        return [column.to_pypika_column(db.type_engine) for column in columns]

    @staticmethod
    def database_to_database(columns, from_db, to_db):
        """
        Transforms columns from one database to Pypika-Columns of another database.

        :param columns: The list of columns from one database.
        :param from_db: The origin database instance.
        :param to_db: The target database instance.
        :return: A list of Pypika-Columns with the target database types.
        """
        ansi_columns = ColumnsTransformer.database_to_ansi(columns, from_db)
        return ColumnsTransformer.ansi_to_database(ansi_columns, to_db)


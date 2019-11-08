import re

from pypika.queries import Column as PypikaColumn


class TypeEngine:
    """
    Base class for database type conversions.
    """
    def __init__(self, db_to_ansi_mapper, ansi_to_db_mapper):
        self.db_to_ansi_mapper = db_to_ansi_mapper
        self.ansi_to_db_mapper = ansi_to_db_mapper

    def to_ansi(self, data_type):
        """
        Translates the provided data type string into an ANSI data type instance.

        :param data_type: The data type string to be translated.
        :return: An instance of an ANSI data type.
        """
        raw_data_type, raw_arguments = self.split_data_type(data_type.lower())

        if raw_data_type not in self.db_to_ansi_mapper:
            raise ValueError('Could not find matching ANSI type for {}.'.format(raw_data_type))

        ansi_data_type = self.db_to_ansi_mapper[raw_data_type]

        return ansi_data_type(*raw_arguments)

    def from_ansi(self, data_type):
        """
        Translates an ANSI data type instance to a database specific representation.

        :param data_type: The data type to be translated.
        :return: A string representation of the database specific data type.
        """
        if data_type not in self.ansi_to_db_mapper:
            raise ValueError('Could not find matching database type for {}.'.format(str(data_type)))

        return self.ansi_to_db_mapper[data_type] + data_type.params

    @staticmethod
    def split_data_type(data_type):
        """
        Splits up a data type string into type name and type arguments.

        :param data_type: The data type string to be split up.
        :return: The raw data type and raw arguments.
        """
        split_data_type = re.findall('\w+', data_type)

        raw_data_type = split_data_type[0] if split_data_type else ''
        raw_arguments = split_data_type[1:] if len(split_data_type) > 1 else []

        return raw_data_type, raw_arguments


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

    def as_database_column(self, type_engine):
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

    def as_pypika_column(self, type_engine):
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
    columns = []
    for column_definition in db_columns:
        column_name = column_definition[0]
        column_type = db_type_engine.to_ansi(column_definition[1])
        columns.append(Column(column_name, column_type))

    return columns

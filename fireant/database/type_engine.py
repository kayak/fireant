import re


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

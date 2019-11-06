class ANSIType:
    """
    Represents an ANSI data type.
    """
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, ANSIType):
            return str(self) == str(other)
        return self.name == str(other)

    def __str__(self):
        return self.name

    @property
    def params(self):
        return ''


class Char(ANSIType):
    """
    Represents the ANSI type CHAR.
    """
    def __init__(self, length=None):
        super(Char, self).__init__(name='CHAR')
        self.length = length

    def __str__(self):
        return '{name}{params}'.format(
              name=self.name,
              params=self.params
        )

    @property
    def params(self):
        return '({length})'.format(length=self.length) if self.length else ''


class VarChar(ANSIType):
    """
    Represents the ANSI type VARCHAR.
    """
    def __init__(self, length=None):
        super(VarChar, self).__init__(name='VARCHAR')
        self.length = length

    def __str__(self):
        return '{name}{params}'.format(
              name=self.name,
              params=self.params
        )

    @property
    def params(self):
        return '({length})'.format(length=self.length) if self.length else ''


class Text(ANSIType):
    """
    Represents the ANSI type TEXT.
    """
    def __init__(self):
        super(Text, self).__init__(name='TEXT')


class Boolean(ANSIType):
    """
    Represents the ANSI type BOOLEAN.
    """
    def __init__(self):
        super(Boolean, self).__init__(name='BOOLEAN')


class Integer(ANSIType):
    """
    Represents the ANSI type INTEGER.
    """
    def __init__(self):
        super(Integer, self).__init__(name='INTEGER')


class SmallInt(ANSIType):
    """
    Represents the ANSI type SMALLINT.
    """
    def __init__(self):
        super(SmallInt, self).__init__(name='SMALLINT')


class BigInt(ANSIType):
    """
    Represents the ANSI type BIGINT.
    """
    def __init__(self):
        super(BigInt, self).__init__(name='BIGINT')


class Decimal(ANSIType):
    """
    Represents the ANSI type DECIMAL.
    """
    def __init__(self, precision=None, scale=None):
        super(Decimal, self).__init__(name='DECIMAL')
        self.precision = precision
        self.scale = scale

    def __str__(self):
        return '{name}{params}'.format(
              name=self.name,
              params=self.params
        )

    @property
    def params(self):
        if not self.precision or not self.scale:
            return ''

        return '({precision},{scale})'.format(
              precision=self.precision,
              scale=self.scale,
        )


class Numeric(ANSIType):
    """
    Represents the ANSI type NUMERIC.
    """
    def __init__(self, precision, scale):
        super(Numeric, self).__init__(name='NUMERIC')
        self.precision = precision
        self.scale = scale

    def __str__(self):
        return '{name}{params}'.format(
              name=self.name,
              params=self.params
        )

    @property
    def params(self):
        if not self.precision or not self.scale:
            return ''

        return '({precision},{scale})'.format(
              precision=self.precision,
              scale=self.scale,
        )


class Float(ANSIType):
    """
    Represents the ANSI type FLOAT.
    """
    def __init__(self, precision):
        super(Float, self).__init__(name='FLOAT')
        self.precision = precision

    def __str__(self):
        return '{name}{params}'.format(
              name=self.name,
              params=self.params
        )

    @property
    def params(self):
        return '({precision})'.format(precision=self.precision) if self.precision else ''


class Real(ANSIType):
    """
    Represents the ANSI type REAL.
    """
    def __init__(self):
        super(Real, self).__init__(name='REAL')


class DoublePrecision(ANSIType):
    """
    Represents the ANSI type DOUBLEPRECISION.
    """
    def __init__(self):
        super(DoublePrecision, self).__init__(name='DOUBLEPRECISION')


class Date(ANSIType):
    """
    Represents the ANSI type DATE.
    """
    def __init__(self):
        super(Date, self).__init__(name='DATE')


class Time(ANSIType):
    """
    Represents the ANSI type TIME.
    """
    def __init__(self):
        super(Time, self).__init__(name='TIME')


class DateTime(ANSIType):
    """
    Represents the ANSI type DATETIME.
    """
    def __init__(self):
        super(DateTime, self).__init__(name='DATETIME')


class Timestamp(ANSIType):
    """
    Represents the ANSI type TIMESTAMP.
    """
    def __init__(self):
        super(Timestamp, self).__init__(name='TIMESTAMP')



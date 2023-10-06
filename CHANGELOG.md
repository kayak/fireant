Changelog is organized by the version of this library, commit date and main points of change

-----
#### [VERSION] - DATE
- Important fix
- Important feature
-----

2023 September

#### [8.0.2] - 2023-09-06
- Specify extras names and the library dependencies for them under tool.poetry.extras

#### [8.0.1] - 2023-09-06
- Describe all the necessary group dependencies in `pyproject.toml` as none of other than Vertica and Snowflake libraries have been included in the published library
- Add newly installed libraries to `poetry.lock`
- Update `pymssql` to 2.2.8

2023 August

#### [8.0.0] - 2023-08-29
- Drop support for Python3.7 as pandas v2 does not support it
- Add Python3.9 support
- Use the newest `pandas` (2.0.3 in requirements) and add the minimum version 2.0.0 of `pandas` to pyproject.toml
- Use the newest `vertica-python` (1.3.4 in requirements) and add the minimum version 1.0.0 of `vertica-python`  to pyproject.toml
- Use the newest `snowflake-connector-python` (3.0.4 in requirements) and add the minimum version 3.0.0 of `snowflake-connector-python` to pyproject.toml
- Use the newest `coverage` (7.3.0 in requirements) and add the minimum version 7.3.0 of `coverage` to pyproject.toml
- Use the newest `watchdog` (3.0.0 in requirements) and add the minimum version 3.0.0 of `watchdiog` to pyproject.toml
- Remove `python-dateutil` from dependencies as it is part of other libraries' dependencies
- Bump `psycopg-binary==2.9.6` though it seems not needed for the tests
- Bump `pymssql==2.2.7` though it seems not needed for the tests
- Bump `Cython==3.0.0` though it seems not needed for the tests
- Get rid of `SyntaxWarning: "is" with a literal. Did you mean "=="?`
- Get rid of `DeprecationWarning: Using or importing the ABCs from 'collections' instead of from 'collections.abc' is deprecated since Python 3.3, and in 3.10 it will stop working`
- Replace `Dataframe.append` to `pd.concat` because `append` does not exist since pandas v2
- Add `group_keys=False` to `DataFrame.groupby()` method because it is no longer ignored since pandas v1.5
- Fix `_apply_share` method for `Share`s with the new libraries.
- Rename `TestDatabase` to `MockDatabase` since it is used only for mocking. This is beneficial also because Python testing method will not delve into it to find methods to run
- Rename `test_connect` and `test_fetch` to `mock_connect` and `mock_fetch` as these are mocks. This is beneficial also because Python testing method will not delve into it to find methods to run
- Rename `TestMySQLDatabase` to `MockMySQLDatabase` for the same reason
- When concatenating `DataFrames`, use `.tail(1)` instead of `.iloc[-1]` as it includes indexes
- Use static CSVs to get the expected `DataFrames` in tests instead of applying methods of `fireant` to a `DataFrame` to get those expected `DataFrames`
- Replace `np.float` to `float` since it was deprecated
- Get rid of `None` and `[]` as `ascending` parameters for `Pandas` class
- Replace `.iteritems()` with `.items()` as the former method was deprecated

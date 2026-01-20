Changelog is organized by the version of this library, commit date and main points of change

-----
#### [VERSION] - DATE
- Important fix
- Important feature
-----

2025 January

#### [8.1.0] - 2025-01-20

**Build System Migration: Poetry → uv + Hatch**

- Rewrote `pyproject.toml` using PEP 621 standard format
- Replaced Poetry's `poetry.lock` with `uv.lock`
- Switched build backend from `poetry.core.masonry.api` to `hatchling.build`
- Removed legacy configuration files: `setup.py`, `setup.cfg`, `tox.ini`, `.coveragerc`
- Removed individual requirements files (`requirements.txt`, `requirements-dev.txt`, `requirements-extras-*.txt`)
- Consolidated all configuration (coverage, pytest, linting) into `pyproject.toml`

**CI/CD Improvements**

- New unified `ci.yml` workflow replaces `black.yml` and `pythonpackage.yml`
  - Updated to GitHub Actions v5 (checkout@v5, setup-uv@v5)
  - Expanded test matrix: Python 3.9, 3.10, 3.11, 3.12, 3.13, 3.14
  - Coverage reporting runs on Python 3.13
- New `release.yml` workflow for automated PyPI publishing using OIDC trusted publishing
- Switched linting from Black to Ruff (same 120 char line length, quote style preserved)

**Pandas 2.x Compatibility Fixes**

- Fixed deprecated `DataFrame.applymap()` → `DataFrame.map()` throughout codebase
- Added `future.no_silent_downcasting` option context to avoid FutureWarnings
- Fixed `groupby()` level parameter to use scalar when single level
- Added `infer_objects(copy=False)` after fillna for proper dtype inference
- Convert DataFrame to object dtype before applying string formatting in pandas widget

**PyPika Compatibility**

- Updated pypika dependency: `0.48.2` (pinned) → `>=0.50.0,<1.0.0`
- Fixed test assertions for PyPika's NULL keyword formatting change

**Other Changes**

- Added pre-commit configuration with Ruff hooks for linting and formatting
- Relaxed `pandas` dependency: `==2.0.3` (pinned) → `>=2.0.0`
- Relaxed `toposort` dependency: `==1.6` (pinned) → `>=1.6,<2`
- Updated `psycopg2-binary` dependency: `>=2.9.0` → `>=2.9.11` (adds Python 3.14 support)
- Dropped Python 3.8 support; minimum Python version is now `>=3.9` (required by psycopg2-binary 2.9.11 and pandas 2.x)
- Snowflake extra is temporarily unavailable on Python 3.14 (upstream `snowflake-connector-python` requires `cffi<2`, which lacks Python 3.14 wheels); Snowflake tests gracefully skip when dependencies are missing
- Fixed invalid escape sequence in `type_engine.py` (was `SyntaxWarning`, will be error in Python 3.16)
- Removed dead code from Snowflake database module
- Configured Ruff to ignore intentional patterns (re-exports in `__init__.py`, star imports in tests)
- Applied Ruff formatting to codebase
- Fixed duplicate test method in `test_build_filters.py`
- Fixed duplicate import in `test_references.py`
- Snowflake: Moved `cryptography.hazmat` imports inside method for lazy loading
- Version now read from package metadata using `importlib.metadata.version()`
- Fixed `filter_nones()` to use lambda instead of `None.__ne__`
- Added `CLAUDE.md` with project overview and development commands
- Added Snowflake and MSSQL database extras to setup documentation
- Added Snowflake database configuration example to documentation
- Removed test API documentation files from docs

2023 December

#### [8.0.5] - 2023-12-18
- Fix dataframe sort with a pivot with the new pandas v2 API
- Optimize it by getting rid of `reset_index` and `set_index` method calls when sorting 

2023 November

#### [8.0.4] - 2023-11-06
Make removal of date total when generating highcharts more flexible by localizing the datetime when filtering out the totals. This will work with timezone aware datetimes as well not aware ones. 

2023 September

#### [8.0.3] - 2023-09-06
- Specify extras dependencies correctly

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

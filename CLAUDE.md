# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FireAnt is a Python analytics and reporting library for building charts, tables, reports, and dashboards. It provides a schema-based approach for configuring metrics and dimensions, abstracting away SQL query construction and chart formatting. Built on top of PyPika (SQL query builder) and Pandas.

## Development Commands

```bash
# Install dependencies (including dev group)
uv sync --group dev

# Install with optional extras (mssql excluded - requires Cython/FreeTDS)
uv sync --group dev --extra mysql --extra postgresql --extra snowflake --extra vertica --extra ipython

# Run all tests
uv run pytest

# Run tests with coverage
uv run coverage run -m pytest && uv run coverage xml

# Run a single test file
uv run pytest fireant/tests/queries/test_builder.py

# Run a specific test class
uv run pytest fireant/tests/queries/test_builder.py::QueryBuilderTests

# Run a specific test method
uv run pytest fireant/tests/queries/test_builder.py::QueryBuilderTests::test_method_name

# Run tests matching a pattern
uv run pytest -k "test_build"

# Lint code
uvx ruff check fireant

# Format code
uvx ruff format fireant

# Build docs
cd docs && uv run sphinx-build -b html . _build/html
```

To update the version, edit `pyproject.toml` (single source of truth). To release, push a tag like `v8.1.0` - GitHub Actions will build and publish to PyPI.

## Code Style

- **Ruff** for linting and formatting: 120 char line length, preserves quote style

## Architecture

### Core Components

- **`fireant/database/`** - Database abstraction layer with connectors for MySQL, PostgreSQL, Redshift, MSSQL, Snowflake, and Vertica. Each database extends `Database` base class.

- **`fireant/dataset/`** - Core DataSet definition:
  - `klass.py` - Main `DataSet` class, the central abstraction
  - `fields.py` - `Field` and `DataType` definitions (date, text, number, boolean)
  - `filters.py` - Filter types (Comparator, Range, Pattern, Contains, Excludes, etc.)
  - `references.py` - Time comparisons (DayOverDay, WeekOverWeek, MonthOverMonth, etc.)
  - `operations.py` - Post-aggregation operations (Share, RollingMean, CumSum, CumProd, CumMean)
  - `intervals.py` - DateTime intervals for grouping (hour, day, week, month, quarter, year)
  - `joins.py` - Join definitions between tables
  - `data_blending.py` - `DataSetBlender` for combining multiple DataSets

- **`fireant/queries/`** - Query building and execution:
  - `builder.py` - Main query builder with fluent API
  - `execution.py` - Query execution against databases
  - `pagination.py` - Result set pagination

- **`fireant/widgets/`** - Output rendering:
  - `pandas.py` - Pandas DataFrame output
  - `highcharts.py` - HighCharts JSON configuration
  - `matplotlib.py` - Matplotlib charts
  - `csv.py` - CSV export
  - `reacttable.py` - React Table JSON

- **`fireant/middleware/`** - Query middleware (ThreadPoolConcurrencyMiddleware for parallel execution, slow query logging)

### Query Flow

1. Define a `DataSet` with database connection, table, fields, and joins
2. Use the `.data` query builder to select dimensions, metrics, filters
3. Add references (time comparisons) and operations
4. Attach widgets for output format
5. Call `.fetch()` to execute and render results

### Key Design Notes

- PyPika's `Term` class is monkey-patched in `__init__.py` to restore hash functionality
- Fields are accessed via `dataset.fields.alias` syntax using a container pattern
- Immutable builder pattern for query construction
- Database drivers are imported lazily (inside methods) so fireant can be imported without all extras installed
- Tests use `pytest` (with unittest-style TestCase classes) and are located in `fireant/tests/` mirroring the main structure

## Database Extras

Install database-specific dependencies:
```bash
uv pip install fireant[mysql]
uv pip install fireant[postgresql]
uv pip install fireant[redshift]
uv pip install fireant[mssql]
uv pip install fireant[snowflake]
uv pip install fireant[vertica]
uv pip install fireant[ipython]  # For Jupyter/Matplotlib support
```

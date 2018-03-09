import copy

from typing import (
    Callable,
    Iterable,
)

from fireant.slicer.references import (
    reference_key,
    reference_term,
)
from pypika import functions as fn
from pypika.queries import QueryBuilder
from pypika.terms import (
    ComplexCriterion,
    Criterion,
    NullValue,
    Term,
)
from ..dimensions import Dimension
from ..intervals import weekly


def make_terms_for_references(references, original_query, ref_query, metrics):
    """
    Makes the terms needed to be selected from a reference query in the container query.

    :param references:
    :param original_query:
    :param ref_query:
    :param metrics:
    :return:
    """
    seen = set()
    terms = []
    for reference in references:
        # Don't select duplicate references twice
        if reference.key in seen \
              and not seen.add(reference.key):
            continue

        # Get function to select reference metrics
        ref_metric = reference_term(reference,
                                    original_query,
                                    ref_query)

        terms += [ref_metric(metric).as_(reference_key(metric, reference))
                  for metric in metrics]

    return terms


def make_dimension_terms_for_reference_container_query(original_query,
                                                       dimensions,
                                                       ref_dimension_definitions):
    """
    Creates a list of terms that will be selected from the reference container query based on the selected dimensions
    and all of the reference queries. Concretely, this will return one term per dimension where the term is a COALESCE
    function call with the dimension defintion as the base value and the reference dimension definitions as the default
    values (COALESCE takes a variable number of args).

    Consequently, the reference dimension definitions should be shifted using the reference's offset function. This
    function expects that to have already been done in the arg ref_dimension_definitions.

    :param original_query:
    :param dimensions:
    :param ref_dimension_definitions:
    :return:
    """

    # Zip these so they are one row per dimension
    ref_dimension_definitions = list(zip(*ref_dimension_definitions))

    terms = []
    for dimension, ref_dimension_definition in zip(dimensions, ref_dimension_definitions):
        term = _select_for_reference_container_query(dimension.key,
                                                     dimension.definition,
                                                     original_query,
                                                     ref_dimension_definition)
        terms.append(term)

        if not dimension.has_display_field:
            continue

        # Select the display definitions as a field from the ref query
        ref_display_definition = [definition.table.field(dimension.display_key)
                                  for definition in ref_dimension_definition]
        display_term = _select_for_reference_container_query(dimension.display_key,
                                                             dimension.display_definition,
                                                             original_query,
                                                             ref_display_definition)
        terms.append(display_term)

    return terms


def make_metric_terms_for_reference_container_query(original_query, metrics):
    return [_select_for_reference_container_query(metric.key, metric.definition, original_query)
            for metric in metrics]


def _select_for_reference_container_query(element_key, element_definition, query, extra_defintions=()):
    """

    :param element_key:
    :param element_definition:
    :param query:
    :param extra_defintions:
    :return:
    """
    if isinstance(element_definition, NullValue):
        # If an element is a literal NULL, then include the definition directly in the container query. It will be
        # omitted from the reference queries
        return element_definition.as_(element_key)

    term = query.field(element_key)

    if extra_defintions:
        term = fn.Coalesce(term, *extra_defintions)

    return term.as_(element_key)


def make_reference_filters(filters, ref_dimension, offset_func):
    """
    Copies and replaces the reference dimension's definition in all of the filters applied to a slicer query.

    This is used to shift the dimension filters to fit the reference window.

    :param filters:
    :param ref_dimension:
    :param offset_func:
    :return:
    """
    offset_ref_dimension_definition = offset_func(ref_dimension.definition)

    reference_filters = []
    for ref_filter in map(copy.deepcopy, filters):
        ref_filter.definition = _apply_to_term_in_criterion(ref_dimension.definition,
                                                            offset_ref_dimension_definition,
                                                            ref_filter.definition)
        reference_filters.append(ref_filter)

    return reference_filters


def make_reference_join_criterion(ref_dimension: Dimension,
                                  all_dimensions: Iterable[Dimension],
                                  original_query: QueryBuilder,
                                  ref_query: QueryBuilder,
                                  offset_func: Callable):
    """
    This creates the criterion for joining a reference query to the base query. It matches the referenced dimension
    in the base query to the offset referenced dimension in the reference query and all other dimensions.

    :param ref_dimension:
        The referenced dimension.
    :param all_dimensions:
        All of the dimensions applied to the slicer query.
    :param original_query:
        The base query, the original query despite the references.
    :param ref_query:
        The reference query, a copy of the base query with the referenced dimension replaced.
    :param offset_func:
        The offset function for shifting the referenced dimension.
    :return:
        pypika.Criterion
            A Criterion that compares all of the dimensions using their terms in the original query and the reference
            query. If the reference dimension is found in the list of dimensions, then it will be shifted using the
            offset function.

            Examples:
                original.date == DATE_ADD(reference.date) AND original.dim1 == reference.dim1
            
        None
            if there are no dimensions. In that case there's nothing to join on and the reference queries should be
            added to the FROM clause of the container query.

    """
    join_criterion = None

    for dimension in all_dimensions:
        ref_query_field = ref_query.field(dimension.key)

        # If this is the reference dimension, it needs to be offset by the reference interval
        if ref_dimension == dimension:
            ref_query_field = offset_func(ref_query_field)

        next_criterion = original_query.field(dimension.key) == ref_query_field

        join_criterion = next_criterion \
            if join_criterion is None \
            else join_criterion & next_criterion

    return join_criterion


def monkey_patch_align_weekdays(database, time_unit, interval):
    def trunc_date(definition, _):
        shift_forward = database.date_add(definition, time_unit, interval)
        # Since we're going to monkey patch the trunc_date function on the database, this needs to call the original
        # function
        offset = database.__class__.trunc_date(database, shift_forward, weekly.key)
        return database.date_add(offset, time_unit, -interval)

    # Copy the database to avoid side effects then monkey patch the trunc date function with the correction for weekday
    database = copy.deepcopy(database)
    database.trunc_date = trunc_date
    return database


def _apply_to_term_in_criterion(target: Term,
                                replacement: Term,
                                criterion: Criterion):
    """
    Finds and replaces a term within a criterion.  This is necessary for adapting filters used in reference queries
    where the reference dimension must be offset by some value.  The target term is found inside the criterion and
    replaced with the replacement.

    :param target:
        The target term to replace in the criterion. It will be replaced in all locations within the criterion with
        the func applied to itself.
    :param replacement:
        The replacement for the term.
    :param criterion:
        The criterion to replace the term in.
    :return:
        A criterion identical to the original criterion arg except with the target term replaced by the replacement arg.
    """
    if isinstance(criterion, ComplexCriterion):
        criterion.left = _apply_to_term_in_criterion(target, replacement, criterion.left)
        criterion.right = _apply_to_term_in_criterion(target, replacement, criterion.right)
        return criterion

    for attr in ['term', 'left', 'right']:
        if hasattr(criterion, attr) and str(getattr(criterion, attr)) == str(target):
            setattr(criterion, attr, replacement)

    return criterion

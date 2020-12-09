from unittest import TestCase

from fireant import *

test_field = Field('my_field', None, label='My Field', prefix='a', suffix='b', precision=2)


class OperationBuilderTests(TestCase):
    def test_alias_derived_from_field(self):
        def test_alias(op, args, expected_alias):
            test_op = op(*args)
            self.assertEqual(expected_alias, test_op.alias)

        with self.subTest(CumSum.__name__):
            test_alias(CumSum, [test_field], 'cumsum(my_field)')
        with self.subTest(CumMean.__name__):
            test_alias(CumMean, [test_field], 'cummean(my_field)')
        with self.subTest(CumProd.__name__):
            test_alias(CumProd, [test_field], 'cumprod(my_field)')
        with self.subTest(RollingMean.__name__):
            test_alias(RollingMean, [test_field, 3], 'rollingmean(my_field,3)')
            test_alias(RollingMean, [test_field, 7], 'rollingmean(my_field,7)')
        with self.subTest(Share.__name__):
            test_dimension = Field('my_other_field', None)
            test_alias(Share, [test_field, test_dimension], 'share(my_field,my_other_field)')

    def test_label_derived_from_field(self):
        def test_label(op, args, expected_label):
            test_op = op(*args)
            self.assertEqual(expected_label, test_op.label)

        with self.subTest(CumSum.__name__):
            test_label(CumSum, [test_field], 'CumSum(My Field)')
        with self.subTest(CumMean.__name__):
            test_label(CumMean, [test_field], 'CumMean(My Field)')
        with self.subTest(CumProd.__name__):
            test_label(CumProd, [test_field], 'CumProd(My Field)')
        with self.subTest(RollingMean.__name__):
            test_label(RollingMean, [test_field, 3], 'RollingMean(My Field,3)')
            test_label(RollingMean, [test_field, 7], 'RollingMean(My Field,7)')
        with self.subTest(Share.__name__):
            test_dimension = Field('my_other_field', None, label='Another Field')
            test_label(Share, [test_field, test_dimension], 'Share of My Field over Another Field')

    def test_share_metric_sets_suffix_as_percent(self):
        test_dimension = Field('my_other_field', None, label='Another Field')
        share = Share(test_field, test_dimension)
        self.assertEqual('%', share.suffix)

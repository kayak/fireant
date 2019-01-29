import copy
from unittest import (
    TestCase,
    skip,
)

from fireant import CumSum
from fireant.slicer.widgets.highcharts import (
    DEFAULT_COLORS,
    HighCharts,
)
from fireant.tests.slicer.mocks import (
    ElectionOverElection,
    cat_dim_df,
    cat_dim_totals_df,
    cat_uni_dim_df,
    cont_dim_df,
    cont_dim_operation_df,
    cont_uni_dim_all_totals_df,
    cont_uni_dim_df,
    cont_uni_dim_ref_delta_df,
    cont_uni_dim_ref_df,
    cont_uni_dim_totals_df,
    multi_metric_df,
    single_metric_df,
    slicer,
)


class HighChartsLineChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.LineSeries
    chart_type = 'line'
    stacking = None

    def test_single_metric_line_chart(self):
        result = HighCharts(title="Time Series, Single Metric") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [(820454400000, 15220449),
                         (946684800000, 16662017),
                         (1072915200000, 19614932),
                         (1199145600000, 21294215),
                         (1325376000000, 20572210),
                         (1451606400000, 18310513)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_metric_prefix_line_chart(self):
        votes = copy.copy(slicer.metrics.votes)
        votes.prefix = '$'
        result = HighCharts(title="Time Series, Single Metric") \
            .axis(self.chart_class(votes)) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [(820454400000, 15220449),
                         (946684800000, 16662017),
                         (1072915200000, 19614932),
                         (1199145600000, 21294215),
                         (1325376000000, 20572210),
                         (1451606400000, 18310513)],
                'tooltip': {
                    'valuePrefix': '$',
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_metric_suffix_line_chart(self):
        votes = copy.copy(slicer.metrics.votes)
        votes.suffix = '%'
        result = HighCharts(title="Time Series, Single Metric") \
            .axis(self.chart_class(votes)) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [(820454400000, 15220449),
                         (946684800000, 16662017),
                         (1072915200000, 19614932),
                         (1199145600000, 21294215),
                         (1325376000000, 20572210),
                         (1451606400000, 18310513)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': '%',
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_metric_precision_line_chart(self):
        votes = copy.copy(slicer.metrics.votes)
        votes.precision = 2
        result = HighCharts(title="Time Series, Single Metric") \
            .axis(self.chart_class(votes)) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [(820454400000, 15220449),
                         (946684800000, 16662017),
                         (1072915200000, 19614932),
                         (1199145600000, 21294215),
                         (1325376000000, 20572210),
                         (1451606400000, 18310513)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': 2,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_single_operation_line_chart(self):
        result = HighCharts(title="Time Series, Single Metric") \
            .axis(self.chart_class(CumSum(slicer.metrics.votes))) \
            .transform(cont_dim_operation_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "CumSum(Votes)",
                "yAxis": "0",
                "data": [(820454400000, 15220449),
                         (946684800000, 31882466),
                         (1072915200000, 51497398),
                         (1199145600000, 72791613),
                         (1325376000000, 93363823),
                         (1451606400000, 111674336)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_single_metric_with_uni_dim_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Single Metric") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Single Metric"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_multi_metrics_single_axis_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DF5353",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "0",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#7798BF",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_multi_metrics_multi_axis_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .axis(self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#DF5353"}},
                "visible": True,
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "1",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DF5353",
                "marker": {"symbol": "circle", "fillColor": "#DF5353"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "1",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#7798BF",
                "marker": {"symbol": "square", "fillColor": "#DF5353"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_multi_dim_with_totals_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .axis(self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_totals_df, slicer, [slicer.dimensions.timestamp,
                                                        slicer.dimensions.state.rollup()], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#7798BF"}},
                "visible": True,
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                'name': 'Votes (Texas)',
                'color': '#DDDF0D',
                'dashStyle': 'Solid',
                'data': [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'circle'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'name': 'Votes (California)',
                'color': '#55BF3B',
                'dashStyle': 'Solid',
                'data': [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'square'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'name': 'Votes (Totals)',
                'color': '#DF5353',
                'dashStyle': 'Solid',
                'data': [(820454400000, 15220449),
                         (946684800000, 16662017),
                         (1072915200000, 19614932),
                         (1199145600000, 21294215),
                         (1325376000000, 20572210),
                         (1451606400000, 18310513)],
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'diamond'},
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'name': 'Wins (Texas)',
                'color': '#7798BF',
                'dashStyle': 'Solid',
                'data': [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#7798BF', 'symbol': 'circle'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'name': 'Wins (California)',
                'color': '#AAEEEE',
                'dashStyle': 'Solid',
                'data': [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#7798BF', 'symbol': 'square'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'name': 'Wins (Totals)',
                'color': '#FF0066',
                'dashStyle': 'Solid',
                'data': [(820454400000, 2),
                         (946684800000, 2),
                         (1072915200000, 2),
                         (1199145600000, 2),
                         (1325376000000, 2),
                         (1451606400000, 2)],
                'marker': {'fillColor': '#7798BF', 'symbol': 'diamond'},
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_multi_dim_with_totals_on_first_dim_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .axis(self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_all_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                            slicer.dimensions.state.rollup()], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#7798BF"}},
                "visible": True,
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                'name': 'Votes (Texas)',
                'color': '#DDDF0D',
                'dashStyle': 'Solid',
                'data': [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'circle'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'name': 'Votes (California)',
                'color': '#55BF3B',
                'dashStyle': 'Solid',
                'data': [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'square'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'name': 'Votes (Totals)',
                'color': '#DF5353',
                'dashStyle': 'Solid',
                'data': [(820454400000, 15220449),
                         (946684800000, 16662017),
                         (1072915200000, 19614932),
                         (1199145600000, 21294215),
                         (1325376000000, 20572210),
                         (1451606400000, 18310513)],
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'diamond'},
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'name': 'Wins (Texas)',
                'color': '#7798BF',
                'dashStyle': 'Solid',
                'data': [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#7798BF', 'symbol': 'circle'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'name': 'Wins (California)',
                'color': '#AAEEEE',
                'dashStyle': 'Solid',
                'data': [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#7798BF', 'symbol': 'square'},
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'name': 'Wins (Totals)',
                'color': '#FF0066',
                'dashStyle': 'Solid',
                'data': [(820454400000, 2),
                         (946684800000, 2),
                         (1072915200000, 2),
                         (1199145600000, 2),
                         (1325376000000, 2),
                         (1451606400000, 2)],
                'marker': {'fillColor': '#7798BF', 'symbol': 'diamond'},
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_uni_dim_with_ref_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Reference") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cont_uni_dim_ref_df,
                       slicer,
                       [
                           slicer.dimensions.timestamp,
                           slicer.dimensions.state
                       ], [
                           ElectionOverElection(slicer.dimensions.timestamp)
                       ])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Reference"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE) (Texas)",
                "yAxis": "0",
                "data": [(946684800000, 5574387),
                         (1072915200000, 6233385),
                         (1199145600000, 7359621),
                         (1325376000000, 8007961),
                         (1451606400000, 7877967)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE) (California)",
                "yAxis": "0",
                "data": [(946684800000, 9646062),
                         (1072915200000, 10428632),
                         (1199145600000, 12255311),
                         (1325376000000, 13286254),
                         (1451606400000, 12694243)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_uni_dim_with_ref_delta_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Delta Reference") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cont_uni_dim_ref_delta_df,
                       slicer,
                       [
                           slicer.dimensions.timestamp,
                           slicer.dimensions.state
                       ], [
                           ElectionOverElection(slicer.dimensions.timestamp, delta=True)
                       ])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Delta Reference"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }, {
                "id": "0_eoe_delta",
                "title": {"text": "EoE Δ"},
                "opposite": True,
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE Δ) (Texas)",
                "yAxis": "0_eoe_delta",
                "data": [(946684800000, -658998),
                         (1072915200000, -1126236),
                         (1199145600000, -648340),
                         (1325376000000, 129994),
                         (1451606400000, 2805052)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE Δ) (California)",
                "yAxis": "0_eoe_delta",
                "data": [(946684800000, -782570),
                         (1072915200000, -1826679),
                         (1199145600000, -1030943),
                         (1325376000000, 592011),
                         (1451606400000, -543355)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_invisible_y_axis(self):
        result = HighCharts(title="Time Series, Single Metric") \
            .axis(self.chart_class(slicer.metrics.votes), y_axis_visible=False) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": False,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [(820454400000, 15220449),
                         (946684800000, 16662017),
                         (1072915200000, 19614932),
                         (1199145600000, 21294215),
                         (1325376000000, 20572210),
                         (1451606400000, 18310513)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_ref_axes_set_to_same_visibility_as_parent_axis(self):
        result = HighCharts(title="Time Series with Unique Dimension and Delta Reference") \
            .axis(self.chart_class(slicer.metrics.votes), y_axis_visible=False) \
            .transform(cont_uni_dim_ref_delta_df,
                       slicer,
                       [
                           slicer.dimensions.timestamp,
                           slicer.dimensions.state
                       ], [
                           ElectionOverElection(slicer.dimensions.timestamp, delta=True)
                       ])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Delta Reference"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": False,
            }, {
                "id": "0_eoe_delta",
                "title": {"text": "EoE Δ"},
                "opposite": True,
                "labels": {"style": {"color": None}},
                "visible": False,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE Δ) (Texas)",
                "yAxis": "0_eoe_delta",
                "data": [(946684800000, -658998),
                         (1072915200000, -1126236),
                         (1199145600000, -648340),
                         (1325376000000, 129994),
                         (1451606400000, 2805052)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE Δ) (California)",
                "yAxis": "0_eoe_delta",
                "data": [(946684800000, -782570),
                         (1072915200000, -1826679),
                         (1199145600000, -1030943),
                         (1325376000000, 592011),
                         (1451606400000, -543355)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)


class HighChartsBarChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.BarSeries
    chart_type = 'bar'
    stacking = None

    def test_single_metric_bar_chart(self):
        result = HighCharts(title="All Votes") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(single_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "All Votes"},
            "xAxis": {
                "type": "category",
                "categories": ["All"],
                'visible': True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [{'x': 0, 'y': 111674336}],
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_multi_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "Votes and Wins"},
            "xAxis": {
                "type": "category",
                "categories": ["All"],
                'visible': True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [{'x': 0, 'y': 111674336}],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins",
                "yAxis": "0",
                "data": [{'x': 0, 'y': 12}],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cat_dim_single_metric_bar_chart(self):
        result = HighCharts("Votes and Wins") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            "title": {"text": "Votes and Wins"},
            "xAxis": {
                "type": "category",
                "categories": ["Democrat", "Independent", "Republican"],
                'visible': True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [{'x': 0, 'y': 54551568},
                         {'x': 1, 'y': 1076384},
                         {'x': 2, 'y': 56046384}],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cat_dim_multi_metric_bar_chart(self):
        result = HighCharts("Votes and Wins") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            "title": {"text": "Votes and Wins"},
            "xAxis": {
                "type": "category",
                "categories": ["Democrat", "Independent", "Republican"],
                'visible': True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [{'x': 0, 'y': 54551568},
                         {'x': 1, 'y': 1076384},
                         {'x': 2, 'y': 56046384}],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins",
                "yAxis": "0",
                "data": [{'x': 0, 'y': 6},
                         {'x': 1, 'y': 0},
                         {'x': 2, 'y': 6}],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cont_uni_dims_single_metric_bar_chart(self):
        result = HighCharts("Election Votes by State") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": True,

            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cont_uni_dims_multi_metric_single_axis_bar_chart(self):
        result = HighCharts(title="Election Votes by State") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "0",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cont_uni_dims_multi_metric_multi_axis_bar_chart(self):
        result = HighCharts(title="Election Votes by State") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .axis(self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#DF5353"}},
                "visible": True,
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}},
                "visible": True,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "1",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "1",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cat_dim_with_totals_chart(self):
        result = HighCharts(title="Categorical Dimension with Totals") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cat_dim_totals_df, slicer, [slicer.dimensions.political_party.rollup()], [])

        self.assertEqual({
            'title': {'text': 'Categorical Dimension with Totals'},
            'xAxis': {
                'categories': ['Democrat', 'Independent', 'Republican', 'Totals'],
                'type': 'category',
                'visible': True
            },
            'yAxis': [{
                'id': '0',
                'labels': {'style': {'color': None}},
                'title': {'text': None},
                'visible': True
            }],
            'legend': {'useHTML': True},
            'series': [{
                'name': 'Votes',
                'yAxis': '0',
                'data': [{'x': 0, 'y': 54551568},
                         {'x': 1, 'y': 1076384},
                         {'x': 2, 'y': 56046384},
                         {'x': 3, 'y': 111674336}],
                'marker': {},
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                'type': self.chart_type,
                'stacking': self.stacking,
            }],
            'tooltip': {'enabled': True, 'shared': True, 'useHTML': True},
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cat_uni_dim_with_missing_categories(self):
        df = cat_uni_dim_df \
            .drop(('d', '1')) \
            .drop(('r', '2')) \
            .drop(('r', '10'))

        result = HighCharts(title="Categorical Dimension with Totals") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(df, slicer, [slicer.dimensions.political_party, slicer.dimensions.candidate], [])

        self.assertEqual({
            'title': {'text': 'Categorical Dimension with Totals'},
            'xAxis': {
                'categories': ['Democrat', 'Independent', 'Republican'],
                'type': 'category',
                'visible': True
            },
            'yAxis': [{
                'id': '0',
                'labels': {'style': {'color': None}},
                'title': {'text': None},
                'visible': True
            }],
            'legend': {'useHTML': True},
            'series': [
                {
                    'name': 'Votes (Al Gore)',
                    'data': [{'x': 0, 'y': 8294949}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                },
                {
                    'name': 'Votes (John Kerry)',
                    'data': [{'x': 0, 'y': 9578189}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                },
                {
                    'name': 'Votes (Barrack Obama)',
                    'data': [{'x': 0, 'y': 24227234}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                },
                {
                    'name': 'Votes (Hillary Clinton)',
                    'data': [{'x': 0, 'y': 4871678}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                },
                {
                    'name': 'Votes (Ross Perot)',
                    'data': [{'x': 1, 'y': 1076384}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                },
                {
                    'name': 'Votes (George Bush)',
                    'data': [{'x': 2, 'y': 18403811}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                },
                {
                    'name': 'Votes (John McCain)',
                    'data': [{'x': 2, 'y': 9491109}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                },
                {
                    'name': 'Votes (Mitt Romney)',
                    'data': [{'x': 2, 'y': 8148082}],
                    'marker': {},
                    'stacking': self.stacking,
                    'tooltip': {
                        'valueDecimals': None,
                        'valuePrefix': None,
                        'valueSuffix': None
                    },
                    'type': self.chart_type,
                    'yAxis': '0'
                }
            ],
            'tooltip': {'enabled': True, 'shared': True, 'useHTML': True},
            "colors": DEFAULT_COLORS,
        }, result)

    def test_invisible_y_axis(self):
        result = HighCharts(title="All Votes") \
            .axis(self.chart_class(slicer.metrics.votes),
                  y_axis_visible=False) \
            .transform(single_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "All Votes"},
            "xAxis": {
                "type": "category",
                "categories": ["All"],
                'visible': True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}},
                "visible": False,
            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [{'x': 0, 'y': 111674336}],
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)


class HighChartsColumnChartTransformerTests(HighChartsBarChartTransformerTests):
    chart_class = HighCharts.ColumnSeries
    chart_type = 'column'


class HighChartsStackedBarChartTransformerTests(HighChartsBarChartTransformerTests):
    maxDiff = None

    chart_class = HighCharts.StackedBarSeries
    chart_type = 'bar'
    stacking = 'normal'


class HighChartsStackedColumnChartTransformerTests(HighChartsBarChartTransformerTests):
    chart_class = HighCharts.StackedColumnSeries
    chart_type = 'column'
    stacking = 'normal'


class HighChartsAreaChartTransformerTests(HighChartsLineChartTransformerTests):
    chart_class = HighCharts.AreaSeries
    chart_type = 'area'


class HighChartsAreaStackedChartTransformerTests(HighChartsAreaChartTransformerTests):
    chart_class = HighCharts.AreaStackedSeries
    stacking = 'normal'


class HighChartsAreaPercentChartTransformerTests(HighChartsAreaChartTransformerTests):
    chart_class = HighCharts.AreaPercentageSeries
    stacking = 'percent'


class HighChartsPieChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.PieSeries
    chart_type = 'pie'

    def test_single_metric_chart(self):
        result = HighCharts(title="All Votes") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(single_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "All Votes"},
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "name": "Votes",
                "type": "pie",
                "data": [{
                    "name": "Votes",
                    "y": 111674336,
                }],
                'tooltip': {
                    'pointFormat': '<span style="color:{point.color}">●</span> '
                                   '{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>',
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
            }],
            'xAxis': {
                'type': 'category',
                'categories': ['All'],
                'visible': True,
            },
            'yAxis': [{
                'id': '0',
                'labels': {'style': {'color': None}},
                'title': {'text': None},
                'visible': True
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_multi_metric_chart(self):
        result = HighCharts(title="Votes and Wins") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "Votes and Wins"},
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "name": "Votes",
                "type": "pie",
                "data": [{
                    "name": "Votes",
                    "y": 111674336,
                }],
                'tooltip': {
                    'pointFormat': '<span style="color:{point.color}">●</span> '
                                   '{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>',
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
            }, {
                "name": "Wins",
                "type": "pie",
                "data": [{
                    "name": "Wins",
                    "y": 12,
                }],
                'tooltip': {
                    'pointFormat': '<span style="color:{point.color}">●</span> '
                                   '{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>',
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
            }],
            'xAxis': {
                'type': 'category',
                'categories': ['All'],
                'visible': True,
            },
            'yAxis': [{
                'id': '0',
                'labels': {'style': {'color': '#DDDF0D'}},
                'title': {'text': None},
                'visible': True
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    def test_cat_dim_single_metric_chart(self):
        result = HighCharts("Votes and Wins") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            'title': {'text': 'Votes and Wins'},
            'tooltip': {'useHTML': True, 'shared': True, 'enabled': True},
            'legend': {'useHTML': True},
            'series': [{
                'name': 'Votes',
                'type': 'pie',
                'data': [
                    {'y': 56046384, 'name': 'Republican'},
                    {'y': 54551568, 'name': 'Democrat'},
                    {'y': 1076384, 'name': 'Independent'},
                ],
                'tooltip': {
                    'pointFormat': '<span style="color:{point.color}">●</span> '
                                   '{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>',
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
            }],
            'yAxis': [{
                'id': '0',
                'labels': {'style': {'color': None}},
                'title': {'text': None},
                'visible': True
            }],
            'xAxis': {
                'type': 'category',
                'categories': ['Democrat', 'Independent', 'Republican'],
                'visible': True,
            },
            "colors": DEFAULT_COLORS,
        }, result)

    @skip
    def test_cat_dim_multi_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            "title": {"text": "Votes and Wins"},
            "xAxis": {
                "type": "category",
                "categories": ["Democrat", "Independent", "Republican"]
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [54551568, 1076384, 56046384],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins",
                "yAxis": "0",
                "data": [6, 0, 6],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }],
            "colors": DEFAULT_COLORS,
        }, result)

    @skip
    def test_cont_uni_dims_single_metric_bar_chart(self):
        result = HighCharts(title="Election Votes by State") \
            .axis(self.chart_class(slicer.metrics.votes)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}

            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }]
        }, result)

    @skip
    def test_cont_uni_dims_multi_metric_single_axis_bar_chart(self):
        result = HighCharts(title="Election Votes by State") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "0",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }]
        }, result)

    @skip
    def test_cont_uni_dims_multi_metric_multi_axis_bar_chart(self):
        result = HighCharts(title="Election Votes by State") \
            .axis(self.chart_class(slicer.metrics.votes),
                  self.chart_class(slicer.metrics.wins)) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {
                "type": "datetime",
                "visible": True,
            },
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#55BF3B"}}
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True, "enabled": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes (Texas)",
                "yAxis": "0",
                "data": [(820454400000, 5574387),
                         (946684800000, 6233385),
                         (1072915200000, 7359621),
                         (1199145600000, 8007961),
                         (1325376000000, 7877967),
                         (1451606400000, 5072915)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [(820454400000, 9646062),
                         (946684800000, 10428632),
                         (1072915200000, 12255311),
                         (1199145600000, 13286254),
                         (1325376000000, 12694243),
                         (1451606400000, 13237598)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "1",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "1",
                "data": [(820454400000, 1),
                         (946684800000, 1),
                         (1072915200000, 1),
                         (1199145600000, 1),
                         (1325376000000, 1),
                         (1451606400000, 1)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                "color": "#DF5353",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }]
        }, result)

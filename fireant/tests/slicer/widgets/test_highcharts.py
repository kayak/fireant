import copy
from unittest import (
    TestCase,
    skip,
)

from fireant import CumSum
from fireant.slicer.widgets.highcharts import HighCharts
from fireant.tests.slicer.mocks import (
    ElectionOverElection,
    cat_dim_df,
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

    chart_class = HighCharts.LineChart
    chart_type = 'line'
    stacking = None

    def test_single_metric_line_chart(self):
        result = HighCharts(title="Time Series, Single Metric",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    def test_metric_prefix_line_chart(self):
        votes = copy.copy(slicer.metrics.votes)
        votes.prefix = '$'
        result = HighCharts(title="Time Series, Single Metric",
                            axes=[self.chart_class([votes])]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    def test_metric_suffix_line_chart(self):
        votes = copy.copy(slicer.metrics.votes)
        votes.suffix = '%'
        result = HighCharts(title="Time Series, Single Metric",
                            axes=[self.chart_class([votes])]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    def test_metric_precision_line_chart(self):
        votes = copy.copy(slicer.metrics.votes)
        votes.precision = 2
        result = HighCharts(title="Time Series, Single Metric",
                            axes=[self.chart_class([votes])]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    def test_single_operation_line_chart(self):
        result = HighCharts(title="Time Series, Single Metric",
                            axes=[self.chart_class([CumSum(slicer.metrics.votes)])]) \
            .transform(cont_dim_operation_df, slicer, [slicer.dimensions.timestamp], [])

        self.assertEqual({
            "title": {"text": "Time Series, Single Metric"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    def test_single_metric_with_uni_dim_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Single Metric",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Single Metric"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }]
        }, result)

    def test_multi_metrics_single_axis_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics",
                            axes=[self.chart_class([slicer.metrics.votes,
                                                    slicer.metrics.wins])]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
                "color": "#DDDF0D",
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
                "color": "#55BF3B",
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
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }]
        }, result)

    def test_multi_metrics_multi_axis_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis",
                            axes=[self.chart_class([slicer.metrics.votes]),
                                  self.chart_class([slicer.metrics.wins]), ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#55BF3B"}}
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
                "color": "#55BF3B",
                "marker": {"symbol": "circle", "fillColor": "#55BF3B"},
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
                "color": "#DF5353",
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
            }]
        }, result)

    def test_multi_dim_with_totals_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis",
                            axes=[self.chart_class([slicer.metrics.votes]),
                                  self.chart_class([slicer.metrics.wins]), ]) \
            .transform(cont_uni_dim_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                        slicer.dimensions.state.rollup()], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#55BF3B"}}
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
            "legend": {"useHTML": True},
            "series": [{
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
                'name': 'Votes (Texas)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
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
                'name': 'Votes (California)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'color': '#DF5353',
                'dashStyle': 'Solid',
                'data': [(820454400000, 15220449),
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
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'diamond'},
                'name': 'Votes (Totals)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'color': '#55BF3B',
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
                'marker': {'fillColor': '#55BF3B', 'symbol': 'circle'},
                'name': 'Wins (Texas)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'color': '#DF5353',
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
                'marker': {'fillColor': '#55BF3B', 'symbol': 'square'},
                'name': 'Wins (California)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'color': '#7798BF',
                'dashStyle': 'Solid',
                'data': [(820454400000, 2),
                         (946684800000, 2),
                         (1072915200000, 2),
                         (1199145600000, 2),
                         (1325376000000, 2),
                         (1451606400000, 2)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#55BF3B', 'symbol': 'diamond'},
                'name': 'Wins (Totals)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }],
        }, result)

    def test_multi_dim_with_totals_on_first_dim_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis",
                            axes=[self.chart_class([slicer.metrics.votes]),
                                  self.chart_class([slicer.metrics.wins]), ]) \
            .transform(cont_uni_dim_all_totals_df, slicer, [slicer.dimensions.timestamp.rollup(),
                                                            slicer.dimensions.state.rollup()], [])

        self.assertEqual({
            "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#55BF3B"}}
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
            "legend": {"useHTML": True},
            "series": [{
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
                'name': 'Votes (Texas)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
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
                'name': 'Votes (California)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'color': '#DF5353',
                'dashStyle': 'Solid',
                'data': [(820454400000, 15220449),
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
                'marker': {'fillColor': '#DDDF0D', 'symbol': 'diamond'},
                'name': 'Votes (Totals)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '0'
            }, {
                'color': '#55BF3B',
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
                'marker': {'fillColor': '#55BF3B', 'symbol': 'circle'},
                'name': 'Wins (Texas)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'color': '#DF5353',
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
                'marker': {'fillColor': '#55BF3B', 'symbol': 'square'},
                'name': 'Wins (California)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }, {
                'color': '#7798BF',
                'dashStyle': 'Solid',
                'data': [(820454400000, 2),
                         (946684800000, 2),
                         (1072915200000, 2),
                         (1199145600000, 2),
                         (1325376000000, 2),
                         (1451606400000, 2)],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
                'marker': {'fillColor': '#55BF3B', 'symbol': 'diamond'},
                'name': 'Wins (Totals)',
                'stacking': self.stacking,
                'type': self.chart_type,
                'yAxis': '1'
            }],
        }, result)

    def test_uni_dim_with_ref_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Reference",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
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
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
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
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }]
        }, result)

    def test_uni_dim_with_ref_delta_line_chart(self):
        result = HighCharts(title="Time Series with Unique Dimension and Delta Reference",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
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
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }, {
                "id": "0_eoe_delta",
                "title": {"text": "EoE Δ"},
                "opposite": True,
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
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
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
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
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Dash",
                "stacking": self.stacking,
            }]
        }, result)


class HighChartsBarChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.BarChart
    chart_type = 'bar'
    stacking = None

    def test_single_metric_bar_chart(self):
        result = HighCharts(title="All Votes",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
            .transform(single_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "All Votes"},
            "xAxis": {
                "type": "category",
                "categories": ["All"]
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}
            }],
            "tooltip": {"shared": True, "useHTML": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [111674336],
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
            }]
        }, result)

    def test_multi_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins",
                            axes=[self.chart_class([slicer.metrics.votes,
                                                    slicer.metrics.wins])]) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "Votes and Wins"},
            "xAxis": {
                "type": "category",
                "categories": ["All"]
            },
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
            "legend": {"useHTML": True},
            "series": [{
                "type": self.chart_type,
                "name": "Votes",
                "yAxis": "0",
                "data": [111674336],
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
                "data": [12],
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

    def test_cat_dim_single_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
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
                "labels": {"style": {"color": None}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    def test_cat_dim_multi_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins",
                            axes=[self.chart_class([slicer.metrics.votes,
                                                    slicer.metrics.wins])]) \
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
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    def test_cont_uni_dims_single_metric_bar_chart(self):
        result = HighCharts(title="Election Votes by State",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
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

    def test_cont_uni_dims_multi_metric_single_axis_bar_chart(self):
        result = HighCharts(title="Election Votes by State",
                            axes=[self.chart_class([slicer.metrics.votes,
                                                    slicer.metrics.wins])]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
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

    def test_cont_uni_dims_multi_metric_multi_axis_bar_chart(self):
        result = HighCharts(title="Election Votes by State",
                            axes=[self.chart_class([slicer.metrics.votes]),
                                  self.chart_class([slicer.metrics.wins])]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#55BF3B"}}
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
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


class HighChartsColumnChartTransformerTests(HighChartsBarChartTransformerTests):
    chart_class = HighCharts.ColumnChart
    chart_type = 'column'


class HighChartsStackedBarChartTransformerTests(HighChartsBarChartTransformerTests):
    maxDiff = None

    chart_class = HighCharts.StackedBarChart
    chart_type = 'bar'
    stacking = 'normal'


class HighChartsStackedColumnChartTransformerTests(HighChartsBarChartTransformerTests):
    chart_class = HighCharts.StackedColumnChart
    chart_type = 'column'
    stacking = 'normal'


class HighChartsAreaChartTransformerTests(HighChartsLineChartTransformerTests):
    chart_class = HighCharts.AreaChart
    chart_type = 'area'


class HighChartsAreaPercentChartTransformerTests(HighChartsAreaChartTransformerTests):
    chart_class = HighCharts.AreaPercentageChart
    stacking = 'percent'


class HighChartsPieChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.PieChart
    chart_type = 'pie'

    def test_single_metric_pie_chart(self):
        result = HighCharts(title="All Votes",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
            .transform(single_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "All Votes"},
            "tooltip": {"shared": True, "useHTML": True},
            "legend": {"useHTML": True},
            "series": [{
                "name": "Votes",
                "type": "pie",
                "data": [{
                    "name": "Votes",
                    "y": 111674336,
                    "color": "#DDDF0D",
                }],
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
            }],
            'xAxis': {'categories': ['All'], 'type': 'category'},
            'yAxis': [],
        }, result)

    def test_multi_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins",
                            axes=[self.chart_class([slicer.metrics.votes,
                                                    slicer.metrics.wins])]) \
            .transform(multi_metric_df, slicer, [], [])

        self.assertEqual({
            "title": {"text": "Votes and Wins"},
            "tooltip": {"shared": True, "useHTML": True},
            "legend": {"useHTML": True},
            "series": [{
                "name": "Votes",
                "type": "pie",
                "data": [{
                    "name": "Votes",
                    "y": 111674336,
                    "color": "#DDDF0D",
                }],
                'tooltip': {
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
                    "color": "#DDDF0D",
                }],
                'tooltip': {
                    'valueDecimals': None,
                    'valuePrefix': None,
                    'valueSuffix': None
                },
            }],
            'xAxis': {'categories': ['All'], 'type': 'category'},
            'yAxis': [],
        }, result)

    def test_cat_dim_single_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party], [])

        self.assertEqual({
            'title': {'text': 'Votes and Wins'},
            'tooltip': {'useHTML': True, 'shared': True},
            'legend': {'useHTML': True},
            'series': [{
                'name': 'Votes',
                'type': 'pie',
                'data': [{
                    'y': 54551568,
                    'name': 'Democrat',
                    'color': '#DDDF0D'
                }, {
                    'y': 1076384,
                    'name': 'Independent',
                    'color': '#55BF3B'
                }, {
                    'y': 56046384,
                    'name': 'Republican',
                    'color': '#DF5353'
                }],
                'tooltip': {
                    'valuePrefix': None,
                    'valueSuffix': None,
                    'valueDecimals': None,
                },
            }],
            'yAxis': [],
            'xAxis': {'categories': ['Democrat', 'Independent', 'Republican'], 'type': 'category'}
        }, result)

    @skip
    def test_cat_dim_multi_metric_bar_chart(self):
        result = HighCharts(title="Votes and Wins",
                            axes=[self.chart_class([slicer.metrics.votes,
                                                    slicer.metrics.wins])]) \
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
            "tooltip": {"shared": True, "useHTML": True},
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
            }]
        }, result)

    @skip
    def test_cont_uni_dims_single_metric_bar_chart(self):
        result = HighCharts(title="Election Votes by State",
                            axes=[self.chart_class([slicer.metrics.votes])]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": None}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
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
        result = HighCharts(title="Election Votes by State",
                            axes=[self.chart_class([slicer.metrics.votes,
                                                    slicer.metrics.wins]), ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
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
        result = HighCharts(title="Election Votes by State",
                            axes=[self.chart_class([slicer.metrics.votes]),
                                  self.chart_class([slicer.metrics.wins]), ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state], [])

        self.assertEqual({
            "title": {"text": "Election Votes by State"},
            "xAxis": {"type": "datetime"},
            "yAxis": [{
                "id": "1",
                "title": {"text": None},
                "labels": {"style": {"color": "#55BF3B"}}
            }, {
                "id": "0",
                "title": {"text": None},
                "labels": {"style": {"color": "#DDDF0D"}}

            }],
            "tooltip": {"shared": True, "useHTML": True},
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

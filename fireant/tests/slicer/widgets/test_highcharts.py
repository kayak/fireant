from unittest import TestCase

from fireant.slicer.widgets.highcharts import HighCharts
from fireant.tests.slicer.mocks import (
    ElectionOverElection,
    cat_dim_df,
    cont_dim_df,
    cont_uni_dim_df,
    cont_uni_dim_ref_delta_df,
    cont_uni_dim_ref_df,
    multi_metric_df,
    single_metric_df,
    slicer,
)


class HighchartsLineChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.LineChart
    chart_type = 'line'
    stacking = None

    def test_single_metric_line_chart(self):
        result = HighCharts("Time Series, Single Metric", axes=[
            self.chart_class(metrics=[slicer.metrics.votes])
        ]) \
            .transform(cont_dim_df, slicer, [slicer.dimensions.timestamp])

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
                "data": [[820454400000.0, 15220449],
                         [946684800000.0, 16662017],
                         [1072915200000.0, 19614932],
                         [1199145600000.0, 21294215],
                         [1325376000000.0, 20572210],
                         [1451606400000.0, 18310513]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }]
        }, result)

    def test_single_metric_with_uni_dim_line_chart(self):
        result = HighCharts("Time Series with Unique Dimension and Single Metric", axes=[
            self.chart_class(metrics=[slicer.metrics.votes])
        ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state])

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
                "data": [[820454400000.0, 5574387],
                         [946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 9646062],
                         [946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)

    def test_multi_metrics_single_axis_line_chart(self):
        result = HighCharts("Time Series with Unique Dimension and Multiple Metrics", axes=[
            self.chart_class(metrics=[slicer.metrics.votes,
                                      slicer.metrics.wins])
        ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state])

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
                "data": [[820454400000.0, 5574387],
                         [946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 9646062],
                         [946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#DDDF0D",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": False,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "0",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#55BF3B",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)

    def test_multi_metrics_multi_axis_line_chart(self):
        result = HighCharts("Time Series with Unique Dimension and Multiple Metrics, Multi-Axis", axes=[
            self.chart_class(metrics=[slicer.metrics.votes]),
            self.chart_class(metrics=[slicer.metrics.wins]),
        ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp,
                                                 slicer.dimensions.state])

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
                "data": [[820454400000.0, 5574387],
                         [946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 9646062],
                         [946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": False,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "1",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#55BF3B",
                "marker": {"symbol": "circle", "fillColor": "#55BF3B"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "1",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#DF5353",
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)

    def test_uni_dim_with_ref_line_chart(self):
        result = HighCharts("Time Series with Unique Dimension and Reference", axes=[
            self.chart_class(metrics=[slicer.metrics.votes])
        ]) \
            .transform(cont_uni_dim_ref_df, slicer, [slicer.dimensions.timestamp.reference(ElectionOverElection),
                                                     slicer.dimensions.state])

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
                "data": [[946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE) (Texas)",
                "yAxis": "0",
                "data": [[946684800000.0, 5574387.0],
                         [1072915200000.0, 6233385.0],
                         [1199145600000.0, 7359621.0],
                         [1325376000000.0, 8007961.0],
                         [1451606400000.0, 7877967.0]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "ShortDash",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": False,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE) (California)",
                "yAxis": "0",
                "data": [[946684800000.0, 9646062.0],
                         [1072915200000.0, 10428632.0],
                         [1199145600000.0, 12255311.0],
                         [1325376000000.0, 13286254.0],
                         [1451606400000.0, 12694243.0]],
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "ShortDash",
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)

    def test_uni_dim_with_ref_delta_line_chart(self):
        result = HighCharts("Time Series with Unique Dimension and Delta Reference", axes=[
            self.chart_class(metrics=[slicer.metrics.votes])
        ]) \
            .transform(cont_uni_dim_ref_delta_df,
                       slicer,
                       [slicer.dimensions.timestamp.reference(ElectionOverElection.delta()),
                        slicer.dimensions.state])

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
                "data": [[946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE Δ) (Texas)",
                "yAxis": "0_eoe_delta",
                "data": [[946684800000.0, -658998.0],
                         [1072915200000.0, -1126236.0],
                         [1199145600000.0, -648340.0],
                         [1325376000000.0, 129994.0],
                         [1451606400000.0, 2805052.0]],
                "color": "#DDDF0D",
                "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                "dashStyle": "ShortDash",
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "Solid",
                "stacking": self.stacking,
                "visible": False,
            }, {
                "type": self.chart_type,
                "name": "Votes (EoE Δ) (California)",
                "yAxis": "0_eoe_delta",
                "data": [[946684800000.0, -782570.0],
                         [1072915200000.0, -1826679.0],
                         [1199145600000.0, -1030943.0],
                         [1325376000000.0, 592011.0],
                         [1451606400000.0, -543355.0]],
                "color": "#55BF3B",
                "marker": {"symbol": "square", "fillColor": "#55BF3B"},
                "dashStyle": "ShortDash",
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)


class HighchartsBarChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.BarChart
    chart_type = 'bar'
    stacking = None

    def test_single_metric_bar_chart(self):
        result = HighCharts("All Votes", axes=[
            self.chart_class(metrics=[slicer.metrics.votes])
        ]) \
            .transform(single_metric_df, slicer, [])

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
                "data": [111674336.0],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }]
        }, result)

    def test_multi_metric_bar_chart(self):
        result = HighCharts("Votes and Wins", axes=[
            self.chart_class(metrics=[slicer.metrics.votes,
                                      slicer.metrics.wins])
        ]) \
            .transform(multi_metric_df, slicer, [])

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
                "data": [111674336.0],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Wins",
                "yAxis": "0",
                "data": [12.0],
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }]
        }, result)

    def test_cat_dim_single_metric_bar_chart(self):
        result = HighCharts("Votes and Wins", axes=[
            self.chart_class(metrics=[slicer.metrics.votes])
        ]) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party])

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
                "data": [54551568.0, 1076384.0, 56046384.0],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }]
        }, result)

    def test_cat_dim_multi_metric_bar_chart(self):
        result = HighCharts("Votes and Wins", axes=[
            self.chart_class(metrics=[slicer.metrics.votes,
                                      slicer.metrics.wins])
        ]) \
            .transform(cat_dim_df, slicer, [slicer.dimensions.political_party])

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
                "data": [54551568.0, 1076384.0, 56046384.0],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Wins",
                "yAxis": "0",
                "data": [6.0, 0.0, 6.0],
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }]
        }, result)

    def test_cont_uni_dims_single_metric_bar_chart(self):
        result = HighCharts("Election Votes by State", axes=[
            self.chart_class(metrics=[slicer.metrics.votes])
        ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state])

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
                "data": [[820454400000.0, 5574387],
                         [946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 9646062],
                         [946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)

    def test_cont_uni_dims_multi_metric_single_axis_bar_chart(self):
        result = HighCharts("Election Votes by State", axes=[
            self.chart_class(metrics=[slicer.metrics.votes,
                                      slicer.metrics.wins]),
        ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state])

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
                "data": [[820454400000.0, 5574387],
                         [946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 9646062],
                         [946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": False,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "0",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)

    def test_cont_uni_dims_multi_metric_multi_axis_bar_chart(self):
        result = HighCharts("Election Votes by State", axes=[
            self.chart_class(metrics=[slicer.metrics.votes]),
            self.chart_class(metrics=[slicer.metrics.wins]),
        ]) \
            .transform(cont_uni_dim_df, slicer, [slicer.dimensions.timestamp, slicer.dimensions.state])

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
                "data": [[820454400000.0, 5574387],
                         [946684800000.0, 6233385],
                         [1072915200000.0, 7359621],
                         [1199145600000.0, 8007961],
                         [1325376000000.0, 7877967],
                         [1451606400000.0, 5072915]],
                "color": "#DDDF0D",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Votes (California)",
                "yAxis": "0",
                "data": [[820454400000.0, 9646062],
                         [946684800000.0, 10428632],
                         [1072915200000.0, 12255311],
                         [1199145600000.0, 13286254],
                         [1325376000000.0, 12694243],
                         [1451606400000.0, 13237598]],
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": False,
            }, {
                "type": self.chart_type,
                "name": "Wins (Texas)",
                "yAxis": "1",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#55BF3B",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": True,
            }, {
                "type": self.chart_type,
                "name": "Wins (California)",
                "yAxis": "1",
                "data": [[820454400000.0, 1],
                         [946684800000.0, 1],
                         [1072915200000.0, 1],
                         [1199145600000.0, 1],
                         [1325376000000.0, 1],
                         [1451606400000.0, 1]],
                "color": "#DF5353",
                "dashStyle": "Solid",
                "marker": {},
                "stacking": self.stacking,
                "visible": False,
            }]
        }, result)


class HighchartsColumnChartTransformerTests(HighchartsBarChartTransformerTests):
    chart_class = HighCharts.ColumnChart
    chart_type = 'column'


class HighchartsStackedBarChartTransformerTests(HighchartsBarChartTransformerTests):
    maxDiff = None

    chart_class = HighCharts.StackedBarChart
    chart_type = 'bar'
    stacking = "normal"


class HighchartsStackedColumnChartTransformerTests(HighchartsBarChartTransformerTests):
    chart_class = HighCharts.StackedColumnChart
    chart_type = 'column'
    stacking = "normal"


class HighchartsAreaChartTransformerTests(HighchartsLineChartTransformerTests):
    chart_class = HighCharts.AreaChart
    chart_type = 'area'


class HighchartsAreaPercentChartTransformerTests(HighchartsLineChartTransformerTests):
    chart_class = HighCharts.AreaPercentageChart
    chart_type = 'area'
    stacking = "normal"


class HighchartsPieChartTransformerTests(TestCase):
    def test_single_metric_pie_chart(self):
        pass

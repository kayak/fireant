from unittest import TestCase

import pandas as pd

from fireant import (
    CumSum,
    Rollup,
)
from fireant.tests.dataset.mocks import (
    ElectionOverElection,
    day,
    dimx0_metricx1_df,
    dimx0_metricx2_df,
    dimx1_date_df,
    dimx1_date_meticx1_votes_df,
    dimx1_date_operation_df,
    dimx1_num_df,
    dimx1_str_df,
    dimx1_str_totals_df,
    dimx2_category_index_str_df,
    dimx2_date_index_str_df,
    dimx2_date_num_df,
    dimx2_date_str_df,
    dimx2_date_str_ref_delta_df,
    dimx2_date_str_ref_df,
    dimx2_date_str_totals_df,
    dimx2_date_str_totalsx2_df,
    dimx2_str_num_df,
    dimx2_str_str_df,
    mock_dataset,
    year,
)
from fireant.widgets.highcharts import (
    DEFAULT_COLORS,
    HighCharts,
)


class HighChartsLineChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.LineSeries
    chart_type = "line"
    stacking = None

    def test_dimx1_metricx1(self):
        result = (
            HighCharts(title="Time Series, Single Metric")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series, Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 15220449),
                            (946684800000, 16662017),
                            (1072915200000, 19614932),
                            (1199145600000, 21294215),
                            (1325376000000, 20572210),
                            (1451606400000, 18310513),
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_dimx1_year(self):
        result = (
            HighCharts(title="Time Series, Single Metric")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx1_date_df, [year(mock_dataset.fields.timestamp)], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series, Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 15220449),
                            (946684800000, 16662017),
                            (1072915200000, 19614932),
                            (1199145600000, 21294215),
                            (1325376000000, 20572210),
                            (1451606400000, 18310513),
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_dimx1_metricx1_suffix(self):
        result = (
            HighCharts(title="Time Series, Single Metric")
            .axis(self.chart_class(mock_dataset.fields.turnout))
            .transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series, Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Turnout",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 50),
                            (946684800000, 50),
                            (1072915200000, 50),
                            (1199145600000, 50),
                            (1325376000000, 50),
                            (1451606400000, 50),
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": "%",
                            "valueDecimals": 2,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_dimx1_metricx1_prefix_precision(self):
        result = (
            HighCharts(title="Time Series, Single Metric")
            .axis(self.chart_class(mock_dataset.fields.wins_with_style))
            .transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series, Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Wins",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 2),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 2),
                        ],
                        "tooltip": {
                            "valuePrefix": "$",
                            "valueSuffix": None,
                            "valueDecimals": 0,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_single_operation_line_chart(self):
        result = (
            HighCharts(title="Time Series, Single Metric")
            .axis(self.chart_class(CumSum(mock_dataset.fields.votes)))
            .transform(
                dimx1_date_operation_df,
                [mock_dataset.fields.timestamp],
                [],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series, Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "CumSum(Votes)",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 15220449),
                            (946684800000, 31882466),
                            (1072915200000, 51497398),
                            (1199145600000, 72791613),
                            (1325376000000, 93363823),
                            (1451606400000, 111674336),
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_single_metric_with_a_split_dimension_dimx2_date_str_line_chart(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        result = (
            HighCharts(
                title="Time Series with Datetime/Text Dimensions and Single Metric",
                split_dimension=dimensions[1],
            )
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_str_df, dimensions, [])
        )

        with self.subTest("returns 3 charts"):
            self.assertEqual(len(result), 3)

        with self.subTest("for Democrat split (1st chart)"):
            self.assertEqual(
                {
                    "chart": {"height": 240},
                    "title": {"text": "Time Series with Datetime/Text Dimensions and Single Metric (Democrat)"},
                    "xAxis": {"type": "datetime", "visible": True},
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "color": "#DDDF0D",
                            "dashStyle": "Solid",
                            "data": [
                                (820454400000, 7579518),
                                (946684800000, 8294949),
                                (1072915200000, 9578189),
                                (1199145600000, 11803106),
                                (1325376000000, 12424128),
                                (1451606400000, 4871678),
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                            "name": "Votes (Democrat)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[0],
            )

        with self.subTest("for Independent split (2nd chart)"):
            self.assertEqual(
                {
                    "chart": {"height": 240},
                    "title": {"text": "Time Series with Datetime/Text Dimensions and Single Metric (Independent)"},
                    "xAxis": {"type": "datetime", "visible": True},
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "color": "#DDDF0D",
                            "dashStyle": "Solid",
                            "data": [
                                (820454400000, 1076384),
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                            "name": "Votes (Independent)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[1],
            )

        with self.subTest("for Republican split (3rd chart)"):
            self.assertEqual(
                {
                    "chart": {"height": 240},
                    "title": {"text": "Time Series with Datetime/Text Dimensions and Single Metric (Republican)"},
                    "xAxis": {"type": "datetime", "visible": True},
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "color": "#DDDF0D",
                            "dashStyle": "Solid",
                            "data": [
                                (820454400000, 6564547),
                                (946684800000, 8367068),
                                (1072915200000, 10036743),
                                (1199145600000, 9491109),
                                (1325376000000, 8148082),
                                (1451606400000, 13438835),
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                            "name": "Votes (Republican)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[2],
            )

    def test_single_metric_with_a_split_dimension_dimx2_date_str_with_totalsx2_line_chart(self):
        dimensions = [
            Rollup(mock_dataset.fields.timestamp),
            Rollup(mock_dataset.fields.political_party),
        ]
        result = (
            HighCharts(
                title="Time Series with Datetime/Text Dimensions, Single Metric and Totals",
                split_dimension=dimensions[1],
            )
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_str_totalsx2_df, dimensions, [])
        )

        with self.subTest("returns 4 charts"):
            self.assertEqual(len(result), 4)

        with self.subTest("for Totals split (4th chart)"):
            self.assertEqual(
                {
                    "chart": {"height": 240},
                    "title": {"text": "Time Series with Datetime/Text Dimensions, Single Metric and Totals (Totals)"},
                    "xAxis": {"type": "datetime", "visible": True},
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "color": "#DDDF0D",
                            "dashStyle": "Solid",
                            "data": [
                                (820454400000, 15220449),
                                (946684800000, 16662017),
                                (1072915200000, 19614932),
                                (1199145600000, 21294215),
                                (1325376000000, 20572210),
                                (1451606400000, 18310513),
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                            "name": "Votes (Totals)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[3],
            )

    def test_single_metric_with_a_split_dimension_dimx2_str_str_line_chart(self):
        dimensions = [
            mock_dataset.fields.political_party,
            mock_dataset.fields["candidate-name"],
        ]
        result = (
            HighCharts(
                title="Time Series with two Text Dimensions and Single Metric",
                split_dimension=dimensions[0],
            )
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_str_str_df, dimensions, [])
        )

        with self.subTest("returns 3 charts"):
            self.assertEqual(len(result), 3)

        with self.subTest("for Democrat split (1st chart)"):
            self.assertEqual(
                {
                    "chart": {"height": 240},
                    "title": {"text": "Time Series with two Text Dimensions and Single Metric (Democrat)"},
                    "xAxis": {
                        "categories": ["Democrat", "Independent", "Republican"],
                        "type": "category",
                        "visible": True,
                    },
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "color": "#DDDF0D",
                            "dashStyle": "Solid",
                            "data": [
                                {"x": 0, "y": 8294949},
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                            "name": "Votes (Al Gore)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#55BF3B",
                            "dashStyle": "Solid",
                            "data": [{"x": 0, "y": 24227234}],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                            "name": "Votes (Barrack Obama)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#DF5353",
                            "dashStyle": "Solid",
                            "data": [{"x": 0, "y": 7579518}],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "diamond"},
                            "name": "Votes (Bill Clinton)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#7798BF",
                            "dashStyle": "Solid",
                            "data": [{"x": 0, "y": 4871678}],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "triangle"},
                            "name": "Votes (Hillary Clinton)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#AAEEEE",
                            "dashStyle": "Solid",
                            "data": [{"x": 0, "y": 9578189}],
                            "marker": {
                                "fillColor": "#DDDF0D",
                                "symbol": "triangle-down",
                            },
                            "name": "Votes (John Kerry)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[0],
            )

        with self.subTest("for Independent split (2nd chart)"):
            self.assertEqual(
                {
                    "chart": {"height": 240},
                    "title": {"text": "Time Series with two Text Dimensions and Single Metric (Independent)"},
                    "xAxis": {
                        "categories": ["Democrat", "Independent", "Republican"],
                        "type": "category",
                        "visible": True,
                    },
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "color": "#DDDF0D",
                            "dashStyle": "Solid",
                            "data": [
                                {"x": 1, "y": 1076384},
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                            "name": "Votes (Ross Perot)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[1],
            )

        with self.subTest("for Republican split (3rd chart)"):
            self.assertEqual(
                {
                    "chart": {"height": 240},
                    "title": {"text": "Time Series with two Text Dimensions and Single Metric (Republican)"},
                    "xAxis": {
                        "categories": ["Democrat", "Independent", "Republican"],
                        "type": "category",
                        "visible": True,
                    },
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "color": "#DDDF0D",
                            "dashStyle": "Solid",
                            "data": [
                                {"x": 2, "y": 6564547},
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                            "name": "Votes (Bob Dole)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#55BF3B",
                            "dashStyle": "Solid",
                            "data": [
                                {"x": 2, "y": 13438835},
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                            "name": "Votes (Donald Trump)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#DF5353",
                            "dashStyle": "Solid",
                            "data": [
                                {"x": 2, "y": 18403811},
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "diamond"},
                            "name": "Votes (George Bush)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#7798BF",
                            "dashStyle": "Solid",
                            "data": [
                                {"x": 2, "y": 9491109},
                            ],
                            "marker": {"fillColor": "#DDDF0D", "symbol": "triangle"},
                            "name": "Votes (John McCain)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                        {
                            "color": "#AAEEEE",
                            "dashStyle": "Solid",
                            "data": [
                                {"x": 2, "y": 8148082},
                            ],
                            "marker": {
                                "fillColor": "#DDDF0D",
                                "symbol": "triangle-down",
                            },
                            "name": "Votes (Mitt Romney)",
                            "stacking": self.stacking,
                            "tooltip": {
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                            "type": self.chart_type,
                            "yAxis": "0",
                        },
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[2],
            )

    def test_multi_metrics_multi_axis_line_chart(self):
        result = (
            HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .axis(self.chart_class(mock_dataset.fields.wins))
            .transform(
                dimx2_date_str_df,
                [mock_dataset.fields.timestamp, mock_dataset.fields.political_party],
                [],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "1",
                        "labels": {"style": {"color": "#7798BF"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                    {
                        "id": "0",
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 7579518),
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                        "data": [(820454400000, 1076384)],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#DF5353",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "diamond"},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#7798BF",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 2),
                            (946684800000, 0),
                            (1072915200000, 0),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 0),
                        ],
                        "marker": {"fillColor": "#7798BF", "symbol": "circle"},
                        "name": "Wins (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#AAEEEE",
                        "dashStyle": "Solid",
                        "data": [(820454400000, 0)],
                        "marker": {"fillColor": "#7798BF", "symbol": "square"},
                        "name": "Wins (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#FF0066",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 0),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 0),
                            (1325376000000, 0),
                            (1451606400000, 2),
                        ],
                        "marker": {"fillColor": "#7798BF", "symbol": "diamond"},
                        "name": "Wins (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_hidden_dim_with_single_metric_line_chart(self):
        result = (
            HighCharts(
                title="Time Series with Hidden Dimension and Single Metric",
                hide=[mock_dataset.fields.political_party],
            )
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx2_date_str_df,
                [mock_dataset.fields.timestamp, mock_dataset.fields.political_party],
                [],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Hidden Dimension and Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 7579518),
                            (820454400000, 1076384),
                            (820454400000, 6564547),
                            (946684800000, 8294949),
                            (946684800000, 8367068),
                            (1072915200000, 9578189),
                            (1072915200000, 10036743),
                            (1199145600000, 11803106),
                            (1199145600000, 9491109),
                            (1325376000000, 12424128),
                            (1325376000000, 8148082),
                            (1451606400000, 4871678),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_hidden_metric_with_single_metric_line_chart(self):
        result = (
            HighCharts(
                title="Time Series with Hidden Metric and Single Metric",
                hide=[mock_dataset.fields.votes],
            )
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx2_date_str_ref_df,
                [mock_dataset.fields.timestamp, mock_dataset.fields.political_party],
                [ElectionOverElection(mock_dataset.fields.timestamp)],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Hidden Metric and Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Dash",
                        "data": [
                            (820454400000, 7579518.0),
                            (946684800000, 6564547.0),
                            (1072915200000, 8367068.0),
                            (1199145600000, 10036743.0),
                            (1325376000000, 9491109.0),
                            (1451606400000, 8148082.0),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes EoE (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        'color': '#55BF3B',
                        'dashStyle': 'Dash',
                        'data': [
                            (946684800000, 1076384.0),
                            (1072915200000, 8294949.0),
                            (1199145600000, 9578189.0),
                            (1325376000000, 11803106.0),
                            (1451606400000, 12424128.0),
                        ],
                        'marker': {'fillColor': '#DDDF0D', 'symbol': 'square'},
                        'name': 'Votes EoE (Democrat)',
                        'stacking': self.stacking,
                        'tooltip': {'valueDecimals': None, 'valuePrefix': None, 'valueSuffix': None},
                        'type': self.chart_type,
                        'yAxis': '0',
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_hidden_ref_with_single_metric_line_chart(self):
        result = (
            HighCharts(
                title="Time Series with Hidden Reference and Single Metric",
                hide=['votes_eoe'],
            )
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx2_date_str_ref_df,
                [mock_dataset.fields.timestamp, mock_dataset.fields.political_party],
                [ElectionOverElection(mock_dataset.fields.timestamp)],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Hidden Reference and Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        'color': '#55BF3B',
                        'dashStyle': 'Solid',
                        'data': [
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        'marker': {'fillColor': '#DDDF0D', 'symbol': 'square'},
                        'name': 'Votes (Democrat)',
                        'stacking': self.stacking,
                        'tooltip': {'valueDecimals': None, 'valuePrefix': None, 'valueSuffix': None},
                        'type': self.chart_type,
                        'yAxis': '0',
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_multi_dim_with_totals_line_chart_and_empty_data(self):
        dataframe = (
            pd.DataFrame()
            .from_dict(
                {
                    "$timestamp": ["~~totals"],
                    "$political_party": ["~~totals"],
                    "$votes": [None],
                    "$wins": [None],
                    "$wins_with_style": [None],
                    "$turnout": [None],
                }
            )
            .set_index(dimx2_date_str_totals_df.index.names)
        )

        result = (
            HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .axis(self.chart_class(mock_dataset.fields.wins))
            .transform(
                dataframe,
                [
                    mock_dataset.fields.timestamp,
                    Rollup(mock_dataset.fields.political_party),
                ],
                [],
            )
        )

        self.assertEqual(
            result,
            {
                "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "1",
                        "title": {"text": None},
                        "labels": {"style": {"color": "#55BF3B"}},
                        "visible": True,
                    },
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "visible": True,
                    },
                ],
                "annotations": [],
                "colors": (
                    "#DDDF0D",
                    "#55BF3B",
                    "#DF5353",
                    "#7798BF",
                    "#AAEEEE",
                    "#FF0066",
                    "#EEAAEE",
                    "#DF5353",
                    "#7798BF",
                    "#AAEEEE",
                ),
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes (Totals)",
                        "data": [],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "yAxis": "0",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "stacking": self.stacking,
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                    },
                    {
                        "type": self.chart_type,
                        "name": "Wins (Totals)",
                        "data": [],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "yAxis": "1",
                        "marker": {"symbol": "circle", "fillColor": "#55BF3B"},
                        "stacking": self.stacking,
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                    },
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
            },
        )

    def test_multi_dim_with_totals_line_chart(self):
        result = (
            HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .axis(self.chart_class(mock_dataset.fields.wins))
            .transform(
                dimx2_date_str_totals_df,
                [
                    mock_dataset.fields.timestamp,
                    Rollup(mock_dataset.fields.political_party),
                ],
                [],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "1",
                        "labels": {"style": {"color": "#AAEEEE"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                    {
                        "id": "0",
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 7579518),
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                        "data": [(820454400000, 1076384)],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#DF5353",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "diamond"},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#7798BF",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 15220449),
                            (946684800000, 16662017),
                            (1072915200000, 19614932),
                            (1199145600000, 21294215),
                            (1325376000000, 20572210),
                            (1451606400000, 18310513),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "triangle"},
                        "name": "Votes (Totals)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#AAEEEE",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 2),
                            (946684800000, 0),
                            (1072915200000, 0),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 0),
                        ],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "circle"},
                        "name": "Wins (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#FF0066",
                        "dashStyle": "Solid",
                        "data": [(820454400000, 0)],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "square"},
                        "name": "Wins (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#EEAAEE",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 0),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 0),
                            (1325376000000, 0),
                            (1451606400000, 2),
                        ],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "diamond"},
                        "name": "Wins (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#DF5353",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 2),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 2),
                        ],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "triangle"},
                        "name": "Wins (Totals)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_multi_dim_with_totals_on_first_dim_line_chart(self):
        result = (
            HighCharts(title="Time Series with Unique Dimension and Multiple Metrics, Multi-Axis")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .axis(self.chart_class(mock_dataset.fields.wins))
            .transform(
                dimx2_date_str_totalsx2_df,
                [
                    Rollup(mock_dataset.fields.timestamp),
                    Rollup(mock_dataset.fields.political_party),
                ],
                [],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Unique Dimension and Multiple Metrics, Multi-Axis"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "1",
                        "labels": {"style": {"color": "#AAEEEE"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                    {
                        "id": "0",
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 7579518),
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                        "data": [(820454400000, 1076384)],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#DF5353",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "diamond"},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#7798BF",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 15220449),
                            (946684800000, 16662017),
                            (1072915200000, 19614932),
                            (1199145600000, 21294215),
                            (1325376000000, 20572210),
                            (1451606400000, 18310513),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "triangle"},
                        "name": "Votes (Totals)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#AAEEEE",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 2),
                            (946684800000, 0),
                            (1072915200000, 0),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 0),
                        ],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "circle"},
                        "name": "Wins (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#FF0066",
                        "dashStyle": "Solid",
                        "data": [(820454400000, 0)],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "square"},
                        "name": "Wins (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#EEAAEE",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 0),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 0),
                            (1325376000000, 0),
                            (1451606400000, 2),
                        ],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "diamond"},
                        "name": "Wins (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "color": "#DF5353",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 2),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 2),
                        ],
                        "marker": {"fillColor": "#AAEEEE", "symbol": "triangle"},
                        "name": "Wins (Totals)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_uni_dim_with_ref_line_chart(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = (
            HighCharts(title="Time Series with Unique Dimension and Reference")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_str_ref_df, dimensions, references)
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Unique Dimension and Reference"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Dash",
                        "data": [
                            (820454400000, 7579518.0),
                            (946684800000, 6564547.0),
                            (1072915200000, 8367068.0),
                            (1199145600000, 10036743.0),
                            (1325376000000, 9491109.0),
                            (1451606400000, 8148082.0),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes EoE (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                        "data": [
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Dash",
                        "data": [
                            (946684800000, 1076384.0),
                            (1072915200000, 8294949.0),
                            (1199145600000, 9578189.0),
                            (1325376000000, 11803106.0),
                            (1451606400000, 12424128.0),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes EoE (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_uni_dim_with_ref_delta_line_chart(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp, delta=True)]
        result = (
            HighCharts(title="Time Series with Unique Dimension and Delta Reference")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_str_ref_delta_df, dimensions, references)
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Unique Dimension and Delta Reference"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    },
                    {
                        "id": "0_eoe_delta",
                        "labels": {"style": {"color": None}},
                        "opposite": True,
                        "title": {"text": "EoE Δ"},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Dash",
                        "data": [
                            (820454400000, 1014971.0),
                            (946684800000, -1802521.0),
                            (1072915200000, -1669675.0),
                            (1199145600000, 545634.0),
                            (1325376000000, 1343027.0),
                            (1451606400000, -5290753.0),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes EoE Δ (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0_eoe_delta",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                        "data": [
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Dash",
                        "data": [
                            (946684800000, -7218565.0),
                            (1072915200000, -1283240.0),
                            (1199145600000, -2224917.0),
                            (1325376000000, -621022.0),
                            (1451606400000, 7552450.0),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes EoE Δ (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0_eoe_delta",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_invisible_y_axis(self):
        result = (
            HighCharts(title="Time Series, Single Metric")
            .axis(self.chart_class(mock_dataset.fields.votes), y_axis_visible=False)
            .transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series, Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": False,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 15220449),
                            (946684800000, 16662017),
                            (1072915200000, 19614932),
                            (1199145600000, 21294215),
                            (1325376000000, 20572210),
                            (1451606400000, 18310513),
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_ref_axes_set_to_same_visibility_as_parent_axis(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp, delta=True)]
        result = (
            HighCharts(title="Time Series with Unique Dimension and Delta Reference")
            .axis(self.chart_class(mock_dataset.fields.votes), y_axis_visible=False)
            .transform(dimx2_date_str_ref_delta_df, dimensions, references)
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Unique Dimension and Delta Reference"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": False,
                    },
                    {
                        "id": "0_eoe_delta",
                        "labels": {"style": {"color": None}},
                        "opposite": True,
                        "title": {"text": "EoE Δ"},
                        "visible": False,
                    },
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#DDDF0D",
                        "dashStyle": "Dash",
                        "data": [
                            (820454400000, 1014971.0),
                            (946684800000, -1802521.0),
                            (1072915200000, -1669675.0),
                            (1199145600000, 545634.0),
                            (1325376000000, 1343027.0),
                            (1451606400000, -5290753.0),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "circle"},
                        "name": "Votes EoE Δ (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0_eoe_delta",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                        "data": [
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Dash",
                        "data": [
                            (946684800000, -7218565.0),
                            (1072915200000, -1283240.0),
                            (1199145600000, -2224917.0),
                            (1325376000000, -621022.0),
                            (1451606400000, 7552450.0),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes EoE Δ (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0_eoe_delta",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )


class HighChartsBarChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.BarSeries
    chart_type = "bar"
    stacking = None

    def test_single_metric_bar_chart(self):
        result = (
            HighCharts(title="All Votes")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx0_metricx1_df, [], [])
        )

        self.assertEqual(
            {
                "title": {"text": "All Votes"},
                "xAxis": {"type": "category", "categories": ["All"], "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [{"x": 0, "y": 111674336}],
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "marker": {},
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_multi_metric_bar_chart(self):
        result = (
            HighCharts(title="Votes and Wins")
            .axis(
                self.chart_class(mock_dataset.fields.votes),
                self.chart_class(mock_dataset.fields.wins),
            )
            .transform(dimx0_metricx2_df, [], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Votes and Wins"},
                "xAxis": {"type": "category", "categories": ["All"], "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [{"x": 0, "y": 111674336}],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "marker": {},
                        "stacking": self.stacking,
                    },
                    {
                        "type": self.chart_type,
                        "name": "Wins",
                        "yAxis": "0",
                        "data": [{"x": 0, "y": 12}],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "marker": {},
                        "stacking": self.stacking,
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_cat_dim_single_metric_bar_chart(self):
        result = (
            HighCharts("Votes and Wins")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx1_str_df, [mock_dataset.fields.political_party], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Votes and Wins"},
                "xAxis": {
                    "type": "category",
                    "categories": ["Democrat", "Independent", "Republican"],
                    "visible": True,
                },
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            {"x": 0, "y": 54551568},
                            {"x": 1, "y": 1076384},
                            {"x": 2, "y": 56046384},
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "marker": {},
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_cat_dim_multi_metric_bar_chart(self):
        result = (
            HighCharts("Votes and Wins")
            .axis(
                self.chart_class(mock_dataset.fields.votes),
                self.chart_class(mock_dataset.fields.wins),
            )
            .transform(dimx1_str_df, [mock_dataset.fields.political_party], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Votes and Wins"},
                "xAxis": {
                    "type": "category",
                    "categories": ["Democrat", "Independent", "Republican"],
                    "visible": True,
                },
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            {"x": 0, "y": 54551568},
                            {"x": 1, "y": 1076384},
                            {"x": 2, "y": 56046384},
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "marker": {},
                        "stacking": self.stacking,
                    },
                    {
                        "type": self.chart_type,
                        "name": "Wins",
                        "yAxis": "0",
                        "data": [{"x": 0, "y": 6}, {"x": 1, "y": 0}, {"x": 2, "y": 6}],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "marker": {},
                        "stacking": self.stacking,
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_cont_uni_dims_single_metric_bar_chart(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        result = (
            HighCharts("Election Votes by State")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_str_df, dimensions, [])
        )

        self.assertEqual(
            {
                "title": {"text": "Election Votes by State"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "data": [
                            (820454400000, 7579518),
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [(820454400000, 1076384)],
                        "marker": {},
                        "name": "Votes (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_cont_uni_dims_multi_metric_single_axis_bar_chart(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        result = (
            HighCharts(title="Election Votes by State")
            .axis(
                self.chart_class(mock_dataset.fields.votes),
                self.chart_class(mock_dataset.fields.wins),
            )
            .transform(dimx2_date_str_df, dimensions, [])
        )

        self.assertEqual(
            {
                "title": {"text": "Election Votes by State"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "data": [
                            (820454400000, 7579518),
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [(820454400000, 1076384)],
                        "marker": {},
                        "name": "Votes (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [
                            (820454400000, 2),
                            (946684800000, 0),
                            (1072915200000, 0),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 0),
                        ],
                        "marker": {},
                        "name": "Wins (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [(820454400000, 0)],
                        "marker": {},
                        "name": "Wins (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [
                            (820454400000, 0),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 0),
                            (1325376000000, 0),
                            (1451606400000, 2),
                        ],
                        "marker": {},
                        "name": "Wins (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_cont_uni_dims_multi_metric_multi_axis_bar_chart(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        result = (
            HighCharts(title="Election Votes by State")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .axis(self.chart_class(mock_dataset.fields.wins))
            .transform(dimx2_date_str_df, dimensions, [])
        )

        self.assertEqual(
            {
                "title": {"text": "Election Votes by State"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "1",
                        "labels": {"style": {"color": "#7798BF"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                    {
                        "id": "0",
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "title": {"text": None},
                        "visible": True,
                    },
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "data": [
                            (820454400000, 7579518),
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "marker": {},
                        "name": "Votes (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [(820454400000, 1076384)],
                        "marker": {},
                        "name": "Votes (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {},
                        "name": "Votes (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [
                            (820454400000, 2),
                            (946684800000, 0),
                            (1072915200000, 0),
                            (1199145600000, 2),
                            (1325376000000, 2),
                            (1451606400000, 0),
                        ],
                        "marker": {},
                        "name": "Wins (Democrat)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "data": [(820454400000, 0)],
                        "marker": {},
                        "name": "Wins (Independent)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                    {
                        "data": [
                            (820454400000, 0),
                            (946684800000, 2),
                            (1072915200000, 2),
                            (1199145600000, 0),
                            (1325376000000, 0),
                            (1451606400000, 2),
                        ],
                        "marker": {},
                        "name": "Wins (Republican)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "1",
                    },
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_cat_dim_with_totals_chart(self):
        result = (
            HighCharts(title="Categorical Dimension with Totals")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx1_str_totals_df,
                [Rollup(mock_dataset.fields.political_party)],
                [],
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Categorical Dimension with Totals"},
                "xAxis": {
                    "categories": ["Democrat", "Independent", "Republican", "Totals"],
                    "type": "category",
                    "visible": True,
                },
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "legend": {"useHTML": True},
                "series": [
                    {
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            {"x": 0, "y": 54551568},
                            {"x": 1, "y": 1076384},
                            {"x": 2, "y": 56046384},
                            {"x": 3, "y": 111674336},
                        ],
                        "marker": {},
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "stacking": self.stacking,
                    }
                ],
                "tooltip": {"enabled": True, "shared": True, "useHTML": True},
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_cat_uni_dim_with_missing_values(self):
        df = dimx2_str_num_df.drop(("Democrat", 1)).drop(("Republican", 2)).drop(("Republican", 10))

        dimensions = [
            mock_dataset.fields.political_party,
            mock_dataset.fields["candidate-id"],
        ]
        result = (
            HighCharts(title="Categorical Dimension with Totals")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(df, dimensions, [])
        )

        self.assertEqual(
            {
                "title": {"text": "Categorical Dimension with Totals"},
                "xAxis": {
                    "categories": ["Democrat", "Independent", "Republican"],
                    "type": "category",
                    "visible": True,
                },
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "legend": {"useHTML": True},
                "series": [
                    {
                        "data": [{"x": 0, "y": 8294949}],
                        "marker": {},
                        "name": "Votes (5)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [{"x": 0, "y": 9578189}],
                        "marker": {},
                        "name": "Votes (6)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [{"x": 0, "y": 24227234}],
                        "marker": {},
                        "name": "Votes (7)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [{"x": 0, "y": 4871678}],
                        "marker": {},
                        "name": "Votes (11)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [{"x": 1, "y": 1076384}],
                        "marker": {},
                        "name": "Votes (3)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [{"x": 2, "y": 18403811}],
                        "marker": {},
                        "name": "Votes (4)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [{"x": 2, "y": 9491109}],
                        "marker": {},
                        "name": "Votes (8)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                    {
                        "data": [{"x": 2, "y": 8148082}],
                        "marker": {},
                        "name": "Votes (9)",
                        "stacking": self.stacking,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": self.chart_type,
                        "yAxis": "0",
                    },
                ],
                "tooltip": {"enabled": True, "shared": False, "useHTML": True},
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_invisible_y_axis(self):
        result = (
            HighCharts(title="All Votes")
            .axis(self.chart_class(mock_dataset.fields.votes), y_axis_visible=False)
            .transform(dimx0_metricx1_df, [], [])
        )

        self.assertEqual(
            {
                "title": {"text": "All Votes"},
                "xAxis": {"type": "category", "categories": ["All"], "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": False,
                    }
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [{"x": 0, "y": 111674336}],
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "marker": {},
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )


class HighChartsColumnChartTransformerTests(HighChartsBarChartTransformerTests):
    chart_class = HighCharts.ColumnSeries
    chart_type = "column"


class HighChartsStackedBarChartTransformerTests(HighChartsBarChartTransformerTests):
    maxDiff = None

    chart_class = HighCharts.StackedBarSeries
    chart_type = "bar"
    stacking = "normal"


class HighChartsStackedColumnChartTransformerTests(HighChartsBarChartTransformerTests):
    chart_class = HighCharts.StackedColumnSeries
    chart_type = "column"
    stacking = "normal"


class HighChartsAreaChartTransformerTests(HighChartsLineChartTransformerTests):
    chart_class = HighCharts.AreaSeries
    chart_type = "area"


class HighChartsAreaStackedChartTransformerTests(HighChartsAreaChartTransformerTests):
    chart_class = HighCharts.AreaStackedSeries
    stacking = "normal"


class HighChartsAreaPercentChartTransformerTests(HighChartsAreaChartTransformerTests):
    chart_class = HighCharts.AreaPercentageSeries
    stacking = "percent"


class HighChartsPieChartTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.PieSeries
    chart_type = "pie"

    def test_pie_chart_metricx1(self):
        result = (
            HighCharts(title="All Votes")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx0_metricx1_df, [], [])
        )

        self.assertEqual(
            {
                "title": {"text": "All Votes"},
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [{"name": "Votes", "y": 111674336}],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">●</span> '
                            "{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    }
                ],
                "xAxis": {"type": "category", "categories": ["All"], "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_pie_chart_dimx2_metricx1_with_a_split_dimension(self):
        result = (
            HighCharts(title="All Votes", split_dimension=mock_dataset.fields.political_party)
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx2_str_str_df, [mock_dataset.fields.political_party, mock_dataset.fields['candidate-name']], []
            )
        )

        with self.subTest("returns 3 charts"):
            self.assertEqual(len(result), 3)

        with self.subTest("for Democrat split (1st chart)"):
            self.assertEqual(
                {
                    'chart': {'height': 240},
                    "title": {"text": "All Votes (Democrat)"},
                    "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "name": "Votes",
                            "type": "pie",
                            "data": [
                                {'name': 'Al Gore', 'y': 8294949},
                                {'name': 'Barrack Obama', 'y': 24227234},
                                {'name': 'Bill Clinton', 'y': 7579518},
                                {'name': 'Hillary Clinton', 'y': 4871678},
                                {'name': 'John Kerry', 'y': 9578189},
                            ],
                            "tooltip": {
                                "pointFormat": '<span style="color:{point.color}">●</span> '
                                "{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                        }
                    ],
                    'xAxis': {
                        'categories': ['Democrat', 'Independent', 'Republican'],
                        'type': 'category',
                        'visible': True,
                    },
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[0],
            )

        with self.subTest("for Independent split (2nd chart)"):
            self.assertEqual(
                {
                    'chart': {'height': 240},
                    "title": {"text": "All Votes (Independent)"},
                    "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "name": "Votes",
                            "type": "pie",
                            "data": [
                                {'name': 'Ross Perot', 'y': 1076384},
                            ],
                            "tooltip": {
                                "pointFormat": '<span style="color:{point.color}">●</span> '
                                "{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                        }
                    ],
                    'xAxis': {
                        'categories': ['Democrat', 'Independent', 'Republican'],
                        'type': 'category',
                        'visible': True,
                    },
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[1],
            )

        with self.subTest("for Republican split (3rd chart)"):
            self.assertEqual(
                {
                    'chart': {'height': 240},
                    "title": {"text": "All Votes (Republican)"},
                    "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                    "legend": {"useHTML": True},
                    "series": [
                        {
                            "name": "Votes",
                            "type": "pie",
                            "data": [
                                {'name': 'Bob Dole', 'y': 6564547},
                                {'name': 'Donald Trump', 'y': 13438835},
                                {'name': 'George Bush', 'y': 18403811},
                                {'name': 'John McCain', 'y': 9491109},
                                {'name': 'Mitt Romney', 'y': 8148082},
                            ],
                            "tooltip": {
                                "pointFormat": '<span style="color:{point.color}">●</span> '
                                "{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                                "valueDecimals": None,
                                "valuePrefix": None,
                                "valueSuffix": None,
                            },
                        }
                    ],
                    'xAxis': {
                        'categories': ['Democrat', 'Independent', 'Republican'],
                        'type': 'category',
                        'visible': True,
                    },
                    "yAxis": [
                        {
                            "id": "0",
                            "labels": {"style": {"color": None}},
                            "title": {"text": None},
                            "visible": True,
                        }
                    ],
                    "annotations": [],
                    "colors": DEFAULT_COLORS,
                },
                result[2],
            )

    def test_pie_chart_metricx2(self):
        result = (
            HighCharts(title="Votes and Wins")
            .axis(
                self.chart_class(mock_dataset.fields.votes),
                self.chart_class(mock_dataset.fields.wins),
            )
            .transform(dimx0_metricx2_df, [], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Votes and Wins"},
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [{"name": "Votes", "y": 111674336}],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">●</span> '
                            "{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    },
                    {
                        "name": "Wins",
                        "type": "pie",
                        "data": [{"name": "Wins", "y": 12}],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">●</span> '
                            "{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    },
                ],
                "xAxis": {"type": "category", "categories": ["All"], "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_pie_chart_dimx1_date(self):
        result = (
            HighCharts("Votes and Wins By Day")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx1_date_df, [mock_dataset.fields.timestamp], [])
        )

        self.assertEqual(
            {
                "annotations": [],
                "colors": DEFAULT_COLORS,
                "legend": {"useHTML": True},
                "series": [
                    {
                        "data": [
                            {"name": "1996-01-01", "y": 15220449},
                            {"name": "2000-01-01", "y": 16662017},
                            {"name": "2004-01-01", "y": 19614932},
                            {"name": "2008-01-01", "y": 21294215},
                            {"name": "2012-01-01", "y": 20572210},
                            {"name": "2016-01-01", "y": 18310513},
                        ],
                        "name": "Votes",
                        "tooltip": {
                            "pointFormat": "<span "
                            'style="color:{point.color}">●</span> '
                            "{series.name}: <b>{point.y} "
                            "({point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": "pie",
                    }
                ],
                "title": {"text": "Votes and Wins By Day"},
                "tooltip": {"enabled": True, "shared": True, "useHTML": True},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
            },
            result,
        )

    def test_pie_chart_dimx1_date_year(self):
        result = (
            HighCharts("Votes and Wins By Day")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx1_date_df, [year(mock_dataset.fields.timestamp)], [])
        )

        self.assertEqual(
            {
                "annotations": [],
                "colors": DEFAULT_COLORS,
                "legend": {"useHTML": True},
                "series": [
                    {
                        "data": [
                            {"name": "1996", "y": 15220449},
                            {"name": "2000", "y": 16662017},
                            {"name": "2004", "y": 19614932},
                            {"name": "2008", "y": 21294215},
                            {"name": "2012", "y": 20572210},
                            {"name": "2016", "y": 18310513},
                        ],
                        "name": "Votes",
                        "tooltip": {
                            "pointFormat": "<span "
                            'style="color:{point.color}">●</span> '
                            "{series.name}: <b>{point.y} "
                            "({point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": "pie",
                    }
                ],
                "title": {"text": "Votes and Wins By Day"},
                "tooltip": {"enabled": True, "shared": True, "useHTML": True},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
            },
            result,
        )

    def test_pie_chart_dimx1_str(self):
        result = (
            HighCharts("Votes and Wins By Party")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx1_str_df, [mock_dataset.fields.political_party], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Votes and Wins By Party"},
                "tooltip": {"useHTML": True, "shared": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [
                            {"y": 54551568, "name": "Democrat"},
                            {"y": 1076384, "name": "Independent"},
                            {"y": 56046384, "name": "Republican"},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">●</span> '
                            "{series.name}: <b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                    }
                ],
                "yAxis": [
                    {
                        "id": "0",
                        "labels": {"style": {"color": None}},
                        "title": {"text": None},
                        "visible": True,
                    }
                ],
                "xAxis": {
                    "type": "category",
                    "categories": ["Democrat", "Independent", "Republican"],
                    "visible": True,
                },
                "annotations": [],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_pie_chart_dimx1_num(self):
        result = (
            HighCharts(title="Votes and Wins By Election")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx1_num_df, [mock_dataset.fields["candidate-id"]], [])
        )

        self.assertEqual(
            {
                "title": {"text": "Votes and Wins By Election"},
                "xAxis": {
                    "type": "category",
                    "categories": [
                        "1",
                        "2",
                        "3",
                        "4",
                        "5",
                        "6",
                        "7",
                        "8",
                        "9",
                        "10",
                        "11",
                    ],
                    "visible": True,
                },
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [
                            {"name": "1", "y": 7579518},
                            {"name": "2", "y": 6564547},
                            {"name": "3", "y": 1076384},
                            {"name": "4", "y": 18403811},
                            {"name": "5", "y": 8294949},
                            {"name": "6", "y": 9578189},
                            {"name": "7", "y": 24227234},
                            {"name": "8", "y": 9491109},
                            {"name": "9", "y": 8148082},
                            {"name": "10", "y": 13438835},
                            {"name": "11", "y": 4871678},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {'
                            "series.name}: <b>{point.y} ({"
                            "point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
            },
            result,
        )

    def test_pie_chart_dimx2_date_str(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        result = (
            HighCharts(title="Votes by Date, Party")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_str_df, dimensions, [])
        )

        self.assertEqual(
            {
                "title": {"text": "Votes by Date, Party"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [
                            {"name": "1996-01-01, Democrat", "y": 7579518},
                            {"name": "1996-01-01, Independent", "y": 1076384},
                            {"name": "1996-01-01, Republican", "y": 6564547},
                            {"name": "2000-01-01, Democrat", "y": 8294949},
                            {"name": "2000-01-01, Republican", "y": 8367068},
                            {"name": "2004-01-01, Democrat", "y": 9578189},
                            {"name": "2004-01-01, Republican", "y": 10036743},
                            {"name": "2008-01-01, Democrat", "y": 11803106},
                            {"name": "2008-01-01, Republican", "y": 9491109},
                            {"name": "2012-01-01, Democrat", "y": 12424128},
                            {"name": "2012-01-01, Republican", "y": 8148082},
                            {"name": "2016-01-01, Democrat", "y": 4871678},
                            {"name": "2016-01-01, Republican", "y": 13438835},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {series.name}: <b>{'
                            "point.y} ({"
                            "point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
            },
            result,
        )

    def test_pie_chart_dimx2_date_num(self):
        dimensions = [
            day(mock_dataset.fields.timestamp),
            mock_dataset.fields["candidate-id"],
        ]
        result = (
            HighCharts(title="Election Votes by Day and Candidate ID")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_num_df, dimensions, [])
        )

        self.assertEqual(
            {
                "title": {"text": "Election Votes by Day and Candidate ID"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [
                            {"name": "1996-01-01, 1", "y": 7579518},
                            {"name": "1996-01-01, 2", "y": 6564547},
                            {"name": "1996-01-01, 3", "y": 1076384},
                            {"name": "2000-01-01, 4", "y": 8367068},
                            {"name": "2000-01-01, 5", "y": 8294949},
                            {"name": "2004-01-01, 4", "y": 10036743},
                            {"name": "2004-01-01, 6", "y": 9578189},
                            {"name": "2008-01-01, 7", "y": 11803106},
                            {"name": "2008-01-01, 8", "y": 9491109},
                            {"name": "2012-01-01, 7", "y": 12424128},
                            {"name": "2012-01-01, 9", "y": 8148082},
                            {"name": "2016-01-01, 10", "y": 13438835},
                            {"name": "2016-01-01, 11", "y": 4871678},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {series.name}: '
                            "<b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
            },
            result,
        )

    def test_pie_chart_dimx2_yearly_date_num(self):
        dimensions = [
            year(mock_dataset.fields.timestamp),
            mock_dataset.fields["candidate-id"],
        ]
        result = (
            HighCharts(title="Election Votes by Day and Candidate ID")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(dimx2_date_num_df, dimensions, [])
        )

        self.assertEqual(
            {
                "title": {"text": "Election Votes by Day and Candidate ID"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [
                            {"name": "1996, 1", "y": 7579518},
                            {"name": "1996, 2", "y": 6564547},
                            {"name": "1996, 3", "y": 1076384},
                            {"name": "2000, 4", "y": 8367068},
                            {"name": "2000, 5", "y": 8294949},
                            {"name": "2004, 4", "y": 10036743},
                            {"name": "2004, 6", "y": 9578189},
                            {"name": "2008, 7", "y": 11803106},
                            {"name": "2008, 8", "y": 9491109},
                            {"name": "2012, 7", "y": 12424128},
                            {"name": "2012, 9", "y": 8148082},
                            {"name": "2016, 10", "y": 13438835},
                            {"name": "2016, 11", "y": 4871678},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {series.name}: '
                            "<b>{point.y} ({point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
            },
            result,
        )

    def test_pie_chart_dimx2_date_str_reference(self):
        dimensions = [
            mock_dataset.fields.timestamp,
            mock_dataset.fields.political_party,
        ]
        references = [ElectionOverElection(mock_dataset.fields.timestamp)]
        result = (
            HighCharts(title="Election Votes by State")
            .axis(
                self.chart_class(mock_dataset.fields.votes),
                self.chart_class(mock_dataset.fields.wins),
            )
            .transform(dimx2_date_str_df, dimensions, references)
        )

        self.assertEqual(
            {
                "title": {"text": "Election Votes by State"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": "#DDDF0D"}},
                        "visible": True,
                    }
                ],
                "annotations": [],
                "colors": DEFAULT_COLORS,
                "series": [
                    {
                        "name": "Votes",
                        "type": "pie",
                        "data": [
                            {"name": "1996-01-01, Democrat", "y": 7579518},
                            {"name": "1996-01-01, Independent", "y": 1076384},
                            {"name": "1996-01-01, Republican", "y": 6564547},
                            {"name": "2000-01-01, Democrat", "y": 8294949},
                            {"name": "2000-01-01, Republican", "y": 8367068},
                            {"name": "2004-01-01, Democrat", "y": 9578189},
                            {"name": "2004-01-01, Republican", "y": 10036743},
                            {"name": "2008-01-01, Democrat", "y": 11803106},
                            {"name": "2008-01-01, Republican", "y": 9491109},
                            {"name": "2012-01-01, Democrat", "y": 12424128},
                            {"name": "2012-01-01, Republican", "y": 8148082},
                            {"name": "2016-01-01, Democrat", "y": 4871678},
                            {"name": "2016-01-01, Republican", "y": 13438835},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {'
                            "series.name}: <b>{point.y} ({"
                            "point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    },
                    {
                        "name": "Votes EoE",
                        "type": "pie",
                        "data": [
                            {"name": "1996-01-01, Democrat", "y": 7579518},
                            {"name": "1996-01-01, Independent", "y": 1076384},
                            {"name": "1996-01-01, Republican", "y": 6564547},
                            {"name": "2000-01-01, Democrat", "y": 8294949},
                            {"name": "2000-01-01, Republican", "y": 8367068},
                            {"name": "2004-01-01, Democrat", "y": 9578189},
                            {"name": "2004-01-01, Republican", "y": 10036743},
                            {"name": "2008-01-01, Democrat", "y": 11803106},
                            {"name": "2008-01-01, Republican", "y": 9491109},
                            {"name": "2012-01-01, Democrat", "y": 12424128},
                            {"name": "2012-01-01, Republican", "y": 8148082},
                            {"name": "2016-01-01, Democrat", "y": 4871678},
                            {"name": "2016-01-01, Republican", "y": 13438835},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {series.name}: <b>{'
                            "point.y} ({"
                            "point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    },
                    {
                        "name": "Wins",
                        "type": "pie",
                        "data": [
                            {"name": "1996-01-01, Democrat", "y": 2},
                            {"name": "1996-01-01, Independent", "y": 0},
                            {"name": "1996-01-01, Republican", "y": 0},
                            {"name": "2000-01-01, Democrat", "y": 0},
                            {"name": "2000-01-01, Republican", "y": 2},
                            {"name": "2004-01-01, Democrat", "y": 0},
                            {"name": "2004-01-01, Republican", "y": 2},
                            {"name": "2008-01-01, Democrat", "y": 2},
                            {"name": "2008-01-01, Republican", "y": 0},
                            {"name": "2012-01-01, Democrat", "y": 2},
                            {"name": "2012-01-01, Republican", "y": 0},
                            {"name": "2016-01-01, Democrat", "y": 0},
                            {"name": "2016-01-01, Republican", "y": 2},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {series.name}: <b>{'
                            "point.y} ({"
                            "point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    },
                    {
                        "name": "Wins EoE",
                        "type": "pie",
                        "data": [
                            {"name": "1996-01-01, Democrat", "y": 2},
                            {"name": "1996-01-01, Independent", "y": 0},
                            {"name": "1996-01-01, Republican", "y": 0},
                            {"name": "2000-01-01, Democrat", "y": 0},
                            {"name": "2000-01-01, Republican", "y": 2},
                            {"name": "2004-01-01, Democrat", "y": 0},
                            {"name": "2004-01-01, Republican", "y": 2},
                            {"name": "2008-01-01, Democrat", "y": 2},
                            {"name": "2008-01-01, Republican", "y": 0},
                            {"name": "2012-01-01, Democrat", "y": 2},
                            {"name": "2012-01-01, Republican", "y": 0},
                            {"name": "2016-01-01, Democrat", "y": 0},
                            {"name": "2016-01-01, Republican", "y": 2},
                        ],
                        "tooltip": {
                            "pointFormat": '<span style="color:{point.color}">\u25cf</span> {series.name}: <b>{'
                            "point.y} ({"
                            "point.percentage:.1f}%)</b><br/>",
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                    },
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
            },
            result,
        )


class HighChartsLineChartAnnotationTransformerTests(TestCase):
    maxDiff = None

    chart_class = HighCharts.LineSeries
    chart_type = "line"
    stacking = None

    def test_dimx1_timeseries_with_annotation(self):
        result = (
            HighCharts(title="Time Series, Single Metric")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx1_date_meticx1_votes_df,
                [mock_dataset.fields.timestamp],
                [],
                dimx2_date_index_str_df,
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series, Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 15220449),
                            (946684800000, 16662017),
                            (1072915200000, 19614932),
                            (1199145600000, 21294215),
                            (1325376000000, 20572210),
                            (1451606400000, 18310513),
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [
                    {
                        "labels": [
                            {
                                "point": {"x": 820454400000, "xAxis": 0},
                                "text": "Bill Clinton, Bob Dole, Ross Perot, " "Bill Clinton, Bob Dole, Ross Perot",
                            },
                            {
                                "point": {"x": 946684800000, "xAxis": 0},
                                "text": "George Bush, Al Gore, George Bush, Al Gore",
                            },
                            {
                                "point": {"x": 1072915200000, "xAxis": 0},
                                "text": "George Bush, John Kerry, George Bush, " "John Kerry",
                            },
                            {
                                "point": {"x": 1199145600000, "xAxis": 0},
                                "text": "Barrack Obama, John McCain, Barrack " "Obama, John McCain",
                            },
                            {
                                "point": {"x": 1325376000000, "xAxis": 0},
                                "text": "Barrack Obama, Mitt Romney, Barrack " "Obama, Mitt Romney",
                            },
                            {
                                "point": {"x": 1451606400000, "xAxis": 0},
                                "text": "Donald Trump, Hillary Clinton, Donald " "Trump, Hillary Clinton",
                            },
                        ]
                    }
                ],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_dimx2_timeseries_with_annotation(self):
        result = (
            HighCharts(title="Time Series with Dimension and Single Metric")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx2_date_str_df,
                [mock_dataset.fields.timestamp, mock_dataset.fields.political_party],
                [],
                dimx2_date_index_str_df,
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Time Series with Dimension and Single Metric"},
                "xAxis": {"type": "datetime", "visible": True},
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": False, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes (Democrat)",
                        "yAxis": "0",
                        "data": [
                            (820454400000, 7579518),
                            (946684800000, 8294949),
                            (1072915200000, 9578189),
                            (1199145600000, 11803106),
                            (1325376000000, 12424128),
                            (1451606400000, 4871678),
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    },
                    {
                        "color": "#55BF3B",
                        "dashStyle": "Solid",
                        "data": [(820454400000, 1076384)],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "square"},
                        "name": "Votes (Independent)",
                        "stacking": None,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": "line",
                        "yAxis": "0",
                    },
                    {
                        "color": "#DF5353",
                        "dashStyle": "Solid",
                        "data": [
                            (820454400000, 6564547),
                            (946684800000, 8367068),
                            (1072915200000, 10036743),
                            (1199145600000, 9491109),
                            (1325376000000, 8148082),
                            (1451606400000, 13438835),
                        ],
                        "marker": {"fillColor": "#DDDF0D", "symbol": "diamond"},
                        "name": "Votes (Republican)",
                        "stacking": None,
                        "tooltip": {
                            "valueDecimals": None,
                            "valuePrefix": None,
                            "valueSuffix": None,
                        },
                        "type": "line",
                        "yAxis": "0",
                    },
                ],
                "annotations": [
                    {
                        "labels": [
                            {
                                "point": {"x": 820454400000, "xAxis": 0},
                                "text": "Bill Clinton, Bob Dole, Ross Perot, " "Bill Clinton, Bob Dole, Ross Perot",
                            },
                            {
                                "point": {"x": 946684800000, "xAxis": 0},
                                "text": "George Bush, Al Gore, George Bush, Al Gore",
                            },
                            {
                                "point": {"x": 1072915200000, "xAxis": 0},
                                "text": "George Bush, John Kerry, George Bush, " "John Kerry",
                            },
                            {
                                "point": {"x": 1199145600000, "xAxis": 0},
                                "text": "Barrack Obama, John McCain, Barrack " "Obama, John McCain",
                            },
                            {
                                "point": {"x": 1325376000000, "xAxis": 0},
                                "text": "Barrack Obama, Mitt Romney, Barrack " "Obama, Mitt Romney",
                            },
                            {
                                "point": {"x": 1451606400000, "xAxis": 0},
                                "text": "Donald Trump, Hillary Clinton, Donald " "Trump, Hillary Clinton",
                            },
                        ]
                    }
                ],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

    def test_dimx1_category_with_annotation(self):
        result = (
            HighCharts(title="Category Series, Single Metric")
            .axis(self.chart_class(mock_dataset.fields.votes))
            .transform(
                dimx1_str_df,
                [mock_dataset.fields.political_party],
                [],
                dimx2_category_index_str_df,
            )
        )

        self.assertEqual(
            {
                "title": {"text": "Category Series, Single Metric"},
                "xAxis": {
                    "categories": ["Democrat", "Independent", "Republican"],
                    "type": "category",
                    "visible": True,
                },
                "yAxis": [
                    {
                        "id": "0",
                        "title": {"text": None},
                        "labels": {"style": {"color": None}},
                        "visible": True,
                    }
                ],
                "tooltip": {"shared": True, "useHTML": True, "enabled": True},
                "legend": {"useHTML": True},
                "series": [
                    {
                        "type": self.chart_type,
                        "name": "Votes",
                        "yAxis": "0",
                        "data": [
                            {"x": 0, "y": 54551568},
                            {"x": 1, "y": 1076384},
                            {"x": 2, "y": 56046384},
                        ],
                        "tooltip": {
                            "valuePrefix": None,
                            "valueSuffix": None,
                            "valueDecimals": None,
                        },
                        "color": "#DDDF0D",
                        "marker": {"symbol": "circle", "fillColor": "#DDDF0D"},
                        "dashStyle": "Solid",
                        "stacking": self.stacking,
                    }
                ],
                "annotations": [
                    {
                        "labels": [
                            {
                                "point": {"x": 0, "xAxis": 0},
                                "text": "Bill Clinton, Al Gore, John Kerry, "
                                "Barrack Obama, Barrack Obama, Hillary "
                                "Clinton, Bill Clinton, Al Gore, John "
                                "Kerry, Barrack Obama, Barrack Obama, "
                                "Hillary Clinton",
                            },
                            {
                                "point": {"x": 1, "xAxis": 0},
                                "text": "Ross Perot, Ross Perot",
                            },
                            {
                                "point": {"x": 2, "xAxis": 0},
                                "text": "Bob Dole, George Bush, George Bush, "
                                "John McCain, Mitt Romney, Donald Trump, "
                                "Bob Dole, George Bush, George Bush, "
                                "John McCain, Mitt Romney, Donald "
                                "Trump",
                            },
                        ]
                    }
                ],
                "colors": DEFAULT_COLORS,
            },
            result,
        )

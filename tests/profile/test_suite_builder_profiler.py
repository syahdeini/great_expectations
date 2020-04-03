import json
import os
from collections import OrderedDict, Hashable

import pytest
from numpy import Infinity

import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.util import file_relative_path
from great_expectations.dataset import Dataset
from great_expectations.datasource import PandasDatasource
from great_expectations.profile.suite_builder_profiler import SuiteBuilderProfiler
from tests.test_utils import expectationSuiteValidationResultSchema

# TODO this
# def test_SuiteBuilderProfiler_with_context(filesystem_csv_data_context):
#     context = filesystem_csv_data_context
#
#     context.create_expectation_suite("default")
#     datasource = context.datasources["rad_datasource"]
#     base_dir = datasource.config["generators"]["subdir_reader"]["base_directory"]
#     batch_kwargs = {
#         "datasource": "rad_datasource",
#         "path": os.path.join(base_dir, "f1.csv"),
#     }
#     batch = context.get_batch(batch_kwargs, "default")
#     expectation_suite, validation_results = SuiteBuilderProfiler.profile(batch)
#
#     assert expectation_suite.expectation_suite_name == "default"
#     assert "SuiteBuilderProfiler" in expectation_suite.meta
#     assert set(expectation_suite.meta["SuiteBuilderProfiler"].keys()) == {
#         "created_by",
#         "created_at",
#         "batch_kwargs",
#     }
#     assert (
#             expectation_suite.meta["SuiteBuilderProfiler"]["batch_kwargs"] == batch_kwargs
#     )
#     for exp in expectation_suite.expectations:
#         assert "SuiteBuilderProfiler" in exp.meta
#         assert "confidence" in exp.meta["SuiteBuilderProfiler"]
#
#     assert set(validation_results.meta.keys()) == {
#         "batch_kwargs",
#         "batch_markers",
#         "batch_parameters",
#         "expectation_suite_name",
#         "great_expectations.__version__",
#         "run_id",
#     }
#
#     assert expectation_suite.meta["notes"] == {
#         "format": "markdown",
#         "content": [
#             """#### This is an _example_ suite
#
# - This suite was made by quickly glancing at 1000 rows of your data.
# - This is **not a production suite**. It is meant to show examples of expectations.
# - Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
# """
#         ]
#     }
#
#     expectation_types = [expectation["expectation_type"] for expectation in expectation_suite.expectations]
#
#     expected_expectation_types = {'expect_table_row_count_to_be_between', 'expect_table_column_count_to_equal',
#                                   'expect_table_columns_to_match_ordered_list', 'expect_column_values_to_not_be_null',
#                                   'expect_column_min_to_be_between', 'expect_column_max_to_be_between',
#                                   'expect_column_mean_to_be_between', 'expect_column_median_to_be_between',
#                                   'expect_column_quantile_values_to_be_between'}
#
#     assert set(expectation_types) == expected_expectation_types

# TODO this
# def test_context_profiler(filesystem_csv_data_context):
#     """
#     This just validates that it's possible to profile using the datasource hook,
#     and have validation results available in the DataContext
#     """
#     context = filesystem_csv_data_context
#
#     assert isinstance(context.datasources["rad_datasource"], PandasDatasource)
#     assert context.list_expectation_suites() == []
#     context.profile_datasource("rad_datasource", profiler=SuiteBuilderProfiler)
#
#     assert len(context.list_expectation_suites()) == 1
#
#     expected_suite_name = "rad_datasource.subdir_reader.f1.SuiteBuilderProfiler"
#     expectation_suite = context.get_expectation_suite(expected_suite_name)
#
#     for exp in expectation_suite.expectations:
#         assert "SuiteBuilderProfiler" in exp.meta
#         assert "confidence" in exp.meta["SuiteBuilderProfiler"]
#
#     assert expectation_suite.expectation_suite_name == expected_suite_name
#     assert "batch_kwargs" in expectation_suite.meta["SuiteBuilderProfiler"]
#
#     assert expectation_suite.meta["notes"] == {
#         "format": "markdown",
#         "content": [
#             """#### This is an _example_ suite
#
# - This suite was made by quickly glancing at 1000 rows of your data.
# - This is **not a production suite**. It is meant to show examples of expectations.
# - Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
# """
#         ]
#     }
#
#     expectation_types = [expectation["expectation_type"] for expectation in expectation_suite.expectations]
#
#     expected_expectation_types = {'expect_table_row_count_to_be_between', 'expect_table_column_count_to_equal',
#                                   'expect_table_columns_to_match_ordered_list', 'expect_column_values_to_not_be_null',
#                                   'expect_column_min_to_be_between', 'expect_column_max_to_be_between',
#                                   'expect_column_mean_to_be_between', 'expect_column_median_to_be_between',
#                                   'expect_column_quantile_values_to_be_between'}
#
#     assert set(expectation_types) == expected_expectation_types

def group_expectations_by_column(expectation_suite):
    columns = {}

    for expectation in expectation_suite.expectations:
        if "column" in expectation.kwargs and isinstance(expectation.kwargs["column"], Hashable):
            column = expectation.kwargs["column"]
        else:
            column = "_nocolumn"
        if column not in columns:
            columns[column] = []
        columns[column].append(expectation)

    return columns


def test_SuiteBuilderProfiler_uses_all_columns_if_no_configuration_on_pandas(
    pandas_dataset,
):
    observed_suite, evrs = SuiteBuilderProfiler().profile(pandas_dataset)
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "kwargs": {"column": "infinities"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"column": "nulls"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_to_exist",
            },
            {
                "kwargs": {"min_value": 6, "max_value": 7},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_row_count_to_be_between",
            },
            {
                "kwargs": {"value": 3},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_column_count_to_equal",
            },
            {
                "kwargs": {"column_list": ["infinities", "nulls", "naturals"]},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_table_columns_to_match_ordered_list",
            },
            {
                "kwargs": {"column": "infinities"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_be_unique",
            },
            {
                "kwargs": {"column": "infinities"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": -Infinity,
                    "max_value": -Infinity,
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_min_to_be_between",
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "min_value": Infinity,
                    "max_value": Infinity,
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_max_to_be_between",
            },
            {
                "kwargs": {"column": "infinities", "min_value": -1, "max_value": 1},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_mean_to_be_between",
            },
            {
                "kwargs": {"column": "infinities", "min_value": -1.0, "max_value": 1.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_median_to_be_between",
            },
            {
                "kwargs": {
                    "column": "infinities",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [-Infinity, -Infinity],
                            [-4.141592653589793, -2.141592653589793],
                            [-1.0, 1.0],
                            [2.141592653589793, 4.141592653589793],
                            [Infinity, Infinity],
                        ],
                    },
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_quantile_values_to_be_between",
            },
            {
                "kwargs": {"column": "nulls"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_be_unique",
            },
            {
                "kwargs": {"column": "nulls", "mostly": 0.4714285714285715},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "kwargs": {"column": "nulls", "min_value": -1.0, "max_value": 1.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_min_to_be_between",
            },
            {
                "kwargs": {"column": "nulls", "min_value": 2.3, "max_value": 4.3},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_max_to_be_between",
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "min_value": 0.6499999999999999,
                    "max_value": 2.65,
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_mean_to_be_between",
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "min_value": 0.6500000000000001,
                    "max_value": 2.6500000000000004,
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_median_to_be_between",
            },
            {
                "kwargs": {
                    "column": "nulls",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [-1.0, 1.0],
                            [0.10000000000000009, 2.1],
                            [1.2000000000000002, 3.2],
                            [1.2000000000000002, 3.2],
                            [2.3, 4.3],
                        ],
                    },
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_quantile_values_to_be_between",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_be_unique",
            },
            {
                "kwargs": {"column": "naturals"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_values_to_not_be_null",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 0.0, "max_value": 2.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_min_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 6.0, "max_value": 8.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_max_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_mean_to_be_between",
            },
            {
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_median_to_be_between",
            },
            {
                "kwargs": {
                    "column": "naturals",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [0.0, 2.0],
                            [2.0, 4.0],
                            [3.0, 5.0],
                            [4.0, 6.0],
                            [6.0, 8.0],
                        ],
                    },
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
                "expectation_type": "expect_column_quantile_values_to_be_between",
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None

    assert observed_suite == expected


def test_SuiteBuilderProfiler_uses_selected_columns_on_pandas(pandas_dataset):
    columns = ["naturals"]
    observed_suite, evrs = SuiteBuilderProfiler().profile(
        pandas_dataset, profiler_configuraton={"columns": columns}
    )
    assert isinstance(observed_suite, ExpectationSuite)

    expected = ExpectationSuite(
        "default",
        data_asset_type="Dataset",
        expectations=[
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "infinities"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "nulls"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "naturals"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 6, "max_value": 7},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_table_column_count_to_equal",
                "kwargs": {"value": 3},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {"column_list": ["infinities", "nulls", "naturals"]},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "naturals"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "naturals"},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {"column": "naturals", "min_value": 0.0, "max_value": 2.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {"column": "naturals", "min_value": 6.0, "max_value": 8.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_mean_to_be_between",
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_median_to_be_between",
                "kwargs": {"column": "naturals", "min_value": 3.0, "max_value": 5.0},
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
            {
                "expectation_type": "expect_column_quantile_values_to_be_between",
                "kwargs": {
                    "column": "naturals",
                    "quantile_ranges": {
                        "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                        "value_ranges": [
                            [0.0, 2.0],
                            [2.0, 4.0],
                            [3.0, 5.0],
                            [4.0, 6.0],
                            [6.0, 8.0],
                        ],
                    },
                },
                "meta": {"SuiteBuilderProfiler": {"confidence": "very low"}},
            },
        ],
    )

    # remove metadata to simplify assertions
    observed_suite.meta = None
    expected.meta = None
    assert observed_suite == expected


def test_SuiteBuilderProfiler_raises_error_on_bad_configuration(dataset):
    print(dataset)
    suite, evrs = SuiteBuilderProfiler().profile(dataset, profiler_configuraton={"columns": ["Name","PClass","Age","Sex","Survived","SexCode"]})
    assert False



def test_snapshot_SuiteBuilderProfiler_on_titanic():
    """
    A snapshot regression test for SuiteBuilderProfiler.
    We are running the profiler on the Titanic dataset
    and comparing the EVRs to ones retrieved from a
    previously stored file.
    """
    batch = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))
    suite, evrs = SuiteBuilderProfiler().profile(batch, profiler_configuraton={"columns": ["Name","PClass","Age","Sex","Survived","SexCode"]})

    # Check to make sure SuiteBuilderProfiler is adding meta.columns with a single "description" field for each column
    assert "columns" in suite.meta
    for k, v in suite.meta["columns"].items():
        assert v == {"description": ""}

    # Note: the above already produces an EVR; rerunning isn't strictly necessary just for EVRs
    evrs = batch.validate(result_format="SUMMARY")

    # THIS IS NOT DEAD CODE. UNCOMMENT TO SAVE A SNAPSHOT WHEN UPDATING THIS TEST
    # with open(file_relative_path(__file__, '../test_sets/expected_evrs_SuiteBuilderProfiler_on_titanic.json'), 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)
    # with open(file_relative_path(__file__, '../render/fixtures/SuiteBuilderProfiler_evrs.json'), 'w+') as file:
    #     json.dump(expectationSuiteValidationResultSchema.dump(evrs), file, indent=2)

    with open(
            file_relative_path(
                __file__, "../test_sets/expected_evrs_SuiteBuilderProfiler_on_titanic.json"
            ),
            "r",
    ) as file:
        expected_evrs = expectationSuiteValidationResultSchema.load(
            json.load(file, object_pairs_hook=OrderedDict)
        )

        # Version and RUN-ID will be different
    del expected_evrs.meta["great_expectations.__version__"]
    del evrs.meta["great_expectations.__version__"]
    del expected_evrs.meta["run_id"]
    del expected_evrs.meta["batch_kwargs"]["ge_batch_id"]
    del evrs.meta["run_id"]
    del evrs.meta["batch_kwargs"]["ge_batch_id"]

    assert expected_evrs == evrs
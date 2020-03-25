import json
import os
from collections import OrderedDict

import pytest

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path
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


def test_snapshot_SuiteBuilderProfiler_on_titanic():
    """
    A snapshot regression test for SuiteBuilderProfiler.
    We are running the profiler on the Titanic dataset
    and comparing the EVRs to ones retrieved from a
    previously stored file.
    """
    batch = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))
    suite, evrs = SuiteBuilderProfiler().build_suite(batch, columns=["Name","PClass","Age","Sex","Survived","SexCode"])

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
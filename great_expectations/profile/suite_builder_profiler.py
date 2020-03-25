from great_expectations.profile.sample_expectations_dataset_profiler import SampleExpectationsDatasetProfiler


class SuiteBuilderProfiler(SampleExpectationsDatasetProfiler):
    """
    Quick'n'dirty attempt at a suite builder that harnesses the logic in sample profiler.

    """
    @classmethod
    def _build_suite(cls, dataset, columns_to_create_expectations_for):
        dataset.set_default_expectation_argument("catch_exceptions", False)

        value = dataset.expect_table_row_count_to_be_between(min_value=0, max_value=None).result["observed_value"]
        dataset.expect_table_row_count_to_be_between(min_value=max(0, value - 10), max_value=value + 10)

        dataset.set_config_value('interactive_evaluation', True)

        columns = dataset.get_table_columns()

        dataset.expect_table_column_count_to_equal(len(columns))
        dataset.expect_table_columns_to_match_ordered_list(columns)

        meta_columns = {}
        for column in columns:
            meta_columns[column] = {"description": ""}

        column_cache = {}

        for column in columns_to_create_expectations_for:
            cardinality = cls._get_column_cardinality_with_caching(dataset, column, column_cache)
            type = cls._get_column_type_with_caching(dataset, column, column_cache)

            if cardinality in ["two", "very few", "few"]:
                cls._create_expectations_for_low_card_column(dataset, column, column_cache)

            elif cardinality in ["many", "very many", "unique"]:
                dataset.expect_column_values_to_be_unique(column)

                if type in ["int", "float"]:
                    cls._create_expectations_for_numeric_column(dataset, column)
                elif type in ["datetime"]:
                    cls._create_expectations_for_datetime_column(dataset, column)
                elif type in ["string", "unknown"]:
                    cls._create_expectations_for_string_column(dataset, column)

        expectation_suite = dataset.get_expectation_suite(suppress_warnings=True, discard_failed_expectations=False)

        if not expectation_suite.meta:
            expectation_suite.meta = {"columns": meta_columns, "notes": {""}}
        else:
            expectation_suite.meta["columns"] = meta_columns

        expectation_suite.meta["notes"] = {
            "format": "markdown",
            "content": [
                """#### This is an _example_ suite

- This suite was made by quickly glancing at 1000 rows of your data.
- This is **not a production suite**. It is meant to show examples of expectations.
- Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
"""
            ]
        }

        return expectation_suite


class SweetSuiteBuilder:
    @classmethod
    def build_suite(cls, suite_name, batch_kwargs, columns=None):
        print(suite_name, batch_kwargs, columns)
        # do stuff
        # return suite


if __name__ == '__main__':
    # TODO I'm not sure this should be a profiler.
    # API might look like:
    builder = SweetSuiteBuilder()
    suite = builder.build_suite(
        suite="my_new_suite",
        batch_kwargs={"datasource": "foo", "table": "users"},
        columns=["id", "country", "region", "other", "interesting", "columns"]
    )
from great_expectations.profile.sample_expectations_dataset_profiler import (
    SampleExpectationsDatasetProfiler,
)


class SuiteBuilderProfiler(SampleExpectationsDatasetProfiler):
    """
    This profiler helps build example suites for columns you care about.
    """

    @classmethod
    def _profile(cls, dataset, configuration):
        dataset.set_default_expectation_argument("catch_exceptions", False)
        dataset = cls._build_table_row_count_expectation(dataset, tolerance=0.1)
        dataset.set_config_value("interactive_evaluation", True)
        dataset = cls._build_table_column_expectations(dataset)

        dataset_columns = dataset.get_table_columns()

        column_cache = {}
        columns_to_create_expectations_for = None
        # TODO error handling on this config dict
        if configuration:
            columns_to_create_expectations_for = configuration.get("columns", None)

        if not columns_to_create_expectations_for:
            columns_to_create_expectations_for = dataset_columns

        for column in columns_to_create_expectations_for:
            cardinality = cls._get_column_cardinality_with_caching(
                dataset, column, column_cache
            )
            type = cls._get_column_type_with_caching(dataset, column, column_cache)

            if cardinality in ["two", "very few", "few"]:
                cls._create_expectations_for_low_card_column(
                    dataset, column, column_cache
                )
            elif cardinality in ["many", "very many", "unique"]:
                # TODO reconsider unique assertion here. Maybe not on floats?
                dataset.expect_column_values_to_be_unique(column)
                if type in ["int", "float"]:
                    cls._create_expectations_for_numeric_column(dataset, column)
                elif type in ["datetime"]:
                    cls._create_expectations_for_datetime_column(dataset, column)
                elif type in ["string", "unknown"]:
                    cls._create_expectations_for_string_column(dataset, column)

        expectation_suite = cls._build_column_description_metadata(dataset)

        # TODO maybe change this metadata
        #         expectation_suite.meta["notes"] = {
        #             "format": "markdown",
        #             "content": [
        #                 """#### This is an _example_ suite
        #
        # - This suite was made by quickly glancing at 1000 rows of your data.
        # - This is **not a production suite**. It is meant to show examples of expectations.
        # - Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
        # """
        #             ]
        #         }

        return expectation_suite

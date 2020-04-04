from great_expectations.profile.sample_expectations_dataset_profiler import (
    SampleExpectationsDatasetProfiler,
)


class SuiteBuilderProfiler(SampleExpectationsDatasetProfiler):
    """
    This profiler helps build course expectations for columns you care about.

    The goal of this profiler is to expedite the process of authoring an
    expectation suite by building possibly relevant expections for columns that
    you care about. You can then easily edit the suite and adjust or delete
    these expectations to hone your new suite.

    Ranges of acceptable values in the expectations created by this profiler
    (for example, the min/max of the value in
    expect_column_values_to_be_between) are created only to demonstrate the
    functionality and should not be taken as the actual ranges. You should
    definitely edit this course suite.

    Configuration is optional, and if not provided, this profiler will create
    expectations for all columns.

    Configuration is a dictionary with a single `columns` key containing a list
    of the column names you want course expectations created for.

    For example, if you had a wide users table and you want expectations on
    three columns, you'd do this:

    suite = SuiteBuilderProfiler().profile(
        dataset,
        {"columns": ["id", "username", "address"]}
    )
    """

    @classmethod
    def _profile(cls, dataset, configuration):
        dataset.set_default_expectation_argument("catch_exceptions", False)
        dataset = cls._build_table_row_count_expectation(dataset, tolerance=0.1)
        dataset.set_config_value("interactive_evaluation", True)
        dataset = cls._build_table_column_expectations(dataset)

        column_cache = {}
        columns_to_create_expectations_for = None
        blacklisted_expectations = None

        if configuration:
            columns_to_create_expectations_for = configuration.get("columns", None)
            blacklisted_expectations = configuration.get(
                "blacklisted_expectations", None
            )

        if not columns_to_create_expectations_for:
            columns_to_create_expectations_for = dataset.get_table_columns()

        for column in columns_to_create_expectations_for:
            cardinality = cls._get_column_cardinality_with_caching(
                dataset, column, column_cache
            )
            column_type = cls._get_column_type_with_caching(
                dataset, column, column_cache
            )

            if cardinality in ["two", "very few", "few"]:
                cls._create_expectations_for_low_card_column(
                    dataset, column, column_cache
                )
            elif cardinality in ["many", "very many", "unique"]:
                # TODO we will want to finesse the number and types of
                #  expectations created here. The simple version is blacklisting
                #  and the more complex version is desired per column type and
                #  cardinality. This deserves more thought on configuration.
                dataset.expect_column_values_to_be_unique(column)

                if column_type in ["int", "float"]:
                    cls._create_expectations_for_numeric_column(dataset, column)
                elif column_type in ["datetime"]:
                    cls._create_expectations_for_datetime_column(dataset, column)
                elif column_type in ["string", "unknown"]:
                    cls._create_expectations_for_string_column(dataset, column)

        if blacklisted_expectations:
            dataset = _remove_table_expectations(dataset, blacklisted_expectations)
            dataset = _remove_column_expectations(dataset, blacklisted_expectations)

        expectation_suite = cls._build_column_description_metadata(dataset)

        return expectation_suite


def _remove_table_expectations(dataset, all_types_to_remove):
    suite = dataset.get_expectation_suite(discard_failed_expectations=False)
    table_expectations = suite.get_table_expectations()
    removals = [
        e.expectation_type
        for e in table_expectations
        if e.expectation_type in all_types_to_remove
    ]
    for expectation in removals:
        try:
            dataset.remove_expectation(expectation_type=expectation)
        except ValueError:
            pass
    return dataset


def _remove_column_expectations(dataset, all_types_to_remove):
    suite = dataset.get_expectation_suite(discard_failed_expectations=False)
    column_expectations = suite.get_column_expectations()
    removals = [
        e.expectation_type
        for e in column_expectations
        if e.expectation_type in all_types_to_remove
    ]
    for column in dataset.get_table_columns():
        for exp in removals:
            try:
                dataset.remove_expectation(expectation_type=exp, column=column)
            except ValueError:
                pass
    return dataset

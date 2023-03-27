import great_expectations as ge
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from ruamel import yaml

from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

# initializing our data context
context = ge.get_context()

#reading region and s3_path
aws_default_region = "ap-southeast-1"
aws_s3_path = "s3://cc-datapipes-dev-apse1-athena-query-results/"

# setting up our connection string & datasource name
connection_string = "awsathena+rest://@athena.{region}.amazonaws.com/ak_test_domain?s3_staging_dir={s3_path}".format(
    region=aws_default_region, s3_path=aws_s3_path)


datasource_dict = {
    "name": "my_awsathena_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "module_name": "great_expectations.execution_engine",
        "connection_string": connection_string,
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
            "module_name": "great_expectations.datasource.data_connector",
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "include_schema_name": "true",
        },
    },
}

# testing datasource config
context.test_yaml_config(yaml_config=yaml.dump(datasource_dict))

# checks if datasource exists & dumps yaml concfig in great_expecations.yml
sanitize_yaml_and_save_datasource(
    context, yaml.dump(datasource_dict), overwrite_existing=True)

# list our data sources
print(
    'successfuly added datasource, here is our list of connected datasources: \n%s' %
    context.list_datasources())

asset_name = "ak_test_domain.ingestor_us_cities"

expectation_suite_name = "28march"
expectation_suite = context.add_or_update_expectation_suite(
    expectation_suite_name=expectation_suite_name
)

batch_request: BatchRequest = BatchRequest(
    datasource_name="my_awsathena_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="ak_test_domain.ingestor_us_cities",
    limit=1000
)

exclude_column_names = []

data_assistant_result = context.assistants.onboarding.run(
    batch_request=batch_request,
    exclude_column_names=exclude_column_names,
    text_columns_rule={
        "candidate_regexes": [],
    }
)

expectation_suite = data_assistant_result.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)

context.add_or_update_expectation_suite(expectation_suite=expectation_suite)

checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}

checkpoint = SimpleCheckpoint(
    f"ak_test_domain_ingestor_us_cities_{expectation_suite_name}",
    context,
    **checkpoint_config,
)
checkpoint_result = checkpoint.run()

# assert checkpoint_result["success"] is True
print(checkpoint_result)

data_assistant_result.plot_metrics()

data_assistant_result.metrics_by_domain

data_assistant_result.plot_expectations_and_metrics()

data_assistant_result.show_expectations_by_domain_type(
    expectation_suite_name=expectation_suite_name
)

data_assistant_result.show_expectations_by_expectation_type(
    expectation_suite_name=expectation_suite_name
)

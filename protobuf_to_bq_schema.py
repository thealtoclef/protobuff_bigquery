import glob
import json
import os
import subprocess
from pathlib import Path
from string import Template
from tempfile import TemporaryDirectory

import jinja2
from proto_schema_parser.ast import Import, Package
from proto_schema_parser.generator import Generator
from proto_schema_parser.parser import Parser

CWD = Path(__file__).parent.absolute()


def generate_bigquery_schema(topic_schema_definition: str) -> str:
    """
    Generates the BigQuery schema based on the given topic schema definition.

    Args:
        topic_schema_definition: The topic schema definition as a string.

    Returns:
        The BigQuery schema as a string.
    """
    # Parse the comments for type overrides
    type_override_template = Template(
        '[(gen_bq_schema.bigquery).type_override = "$type"]'
    )
    lines = [line.split("//") for line in topic_schema_definition.split("\n")]
    processed_lines = [
        line[0].strip().rstrip(";")
        + " "
        + type_override_template.substitute(type=line[1].strip())
        + ";"
        if len(line) > 1
        else line[0]
        for line in lines
    ]
    topic_schema_definition = "\n".join(processed_lines)

    # Parse the schema
    proto_schema = Parser().parse(topic_schema_definition)
    proto_schema.file_elements.extend(
        [
            Package("pubsub"),
            Import("bq_table.proto"),
            Import("bq_field.proto"),
        ]
    )

    # Write the pubsub.proto file
    with open("target/pubsub.proto", "w") as file:
        file.write(Generator().generate(proto_schema))

    # Generate the pubsub.schema file
    subprocess.run(
        [
            "protoc",
            "--bq-schema_out=target",
            "--bq-schema_opt=single-message",
            "target/pubsub.proto",
        ],
    )

    bq_schema = json.load(open("target/pubsub/pubsub.schema"))
    bq_schema.extend(
        [
            {"name": "subscription_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "message_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "publish_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "attributes", "type": "JSON", "mode": "NULLABLE"},
        ]
    )

    return json.dumps(bq_schema, indent=2)


def generate_template_variables(
    resource_id: str,
    protobuf_schema: str,
    partitioning_field: str = "publish_time",
    clustering_fields: list[str] | None = None,
) -> dict:
    if clustering_fields is not None:
        clustering_fields = clustering_fields[:4]

    # Extract the project_id, dataset_id, and table_id from the resource_id
    resource_id = resource_id.strip(" `\"'")
    project_id, dataset_id, table_id = resource_id.split(".")

    # Generate the BigQuery schema
    schema = generate_bigquery_schema(protobuf_schema)

    return {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "schema": schema,
        "partitioning_field": partitioning_field,
        "clustering_fields": clustering_fields,
    }


def render_google_bigquery_dataset(
    template_variables: dict,
) -> str:
    # Render the Terraform template
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(f"{CWD}/templates"))
    template = environment.get_template("google_bigquery_dataset.yaml.jinja")
    content = template.render(
        project_id=template_variables["project_id"],
        dataset_id=template_variables["dataset_id"],
    )

    return content


def render_google_bigquery_table(
    template_variables: dict,
) -> str:
    # Render the Terraform template
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(f"{CWD}/templates"))
    template = environment.get_template("google_bigquery_table.yaml.jinja")
    content = template.render(
        project_id=template_variables["project_id"],
        dataset_id=template_variables["dataset_id"],
        table_id=template_variables["table_id"],
        schema=template_variables["schema"],
        partitioning_field=template_variables["partitioning_field"],
        clustering_fields=template_variables["clustering_fields"],
    )

    return content


# Example usage
if __name__ == "__main__":
    bigquery_table = "northstar-as-se1-dev.mqtt.event_fcm"
    topic_schema_definition = "syntax = \"proto2\";\nmessage EventTracking {\nrequired string event_name = 1;\noptional string params = 2;// JSON\nrequired string event_id = 3;\noptional string client_id = 4;\nrequired string created_at = 5;// TIMESTAMP\nmap<string, string> map_params = 6;}"
    template_vars = generate_template_variables(
        resource_id=bigquery_table,
        protobuf_schema=topic_schema_definition,
        partitioning_field="created_at",
    )

    with open("target/dataset.yaml", "w") as f:
        f.write(render_google_bigquery_dataset(template_vars))
    with open("target/table.yaml", "w") as f:
        f.write(render_google_bigquery_table(template_vars))

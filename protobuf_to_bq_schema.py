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

    with TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        # Get a list of all .proto files in the protos directory
        proto_files = glob.glob(str(CWD / "protos" / "*.proto"))

        # Create a symlink for each .proto file
        for proto_file in proto_files:
            file_name = os.path.basename(proto_file)
            os.symlink(proto_file, temp_dir / file_name)

        # Write the pubsub.proto file
        with open(temp_dir / "pubsub.proto", "w") as file:
            file.write(Generator().generate(proto_schema))

        # Generate the pubsub.schema file
        subprocess.run(
            [
                "protoc",
                f"--bq-schema_out=.",
                "--bq-schema_opt=single-message",
                f"pubsub.proto",
            ],
            cwd=temp_dir,
        )

        bq_schema = json.load(open(temp_dir / "pubsub" / "pubsub.schema"))
        bq_schema.extend(
            [
                {"name": "subscription_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "message_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "publish_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "attributes", "type": "JSON", "mode": "NULLABLE"},
            ]
        )

        return json.dumps(bq_schema, indent=2)


def render_google_bigquery_table(
    resource_id: str,
    protobuf_schema: str,
    partitioning_field: str = "publish_time",
    clustering_fields: list[str] | None = None,
) -> str:
    """
    Renders the Google BigQuery table Terraform template.

    Args:
        resource_id: The resource ID of the BigQuery table.
        protobuf_schema: The protobuf schema as a string.
        partitioning_field: The partitioning field for the table.
        clustering_fields: The clustering fields for the table.

    Returns:
        The rendered Terraform template as a string.
    """
    # The maximum number of clustering fields is 4 if specified
    if clustering_fields is not None:
        clustering_fields = clustering_fields[:4]

    # Extract the project_id, dataset_id, and table_id from the resource_id
    resource_id = resource_id.strip(" `\"'")
    project_id, dataset_id, table_id = resource_id.split(".")

    # Generate the BigQuery schema
    schema = generate_bigquery_schema(protobuf_schema)

    # Render the Terraform template
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(f"{CWD}/templates"))
    template = environment.get_template("google_bigquery_table.tf.jinja")
    content = template.render(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=schema,
        partitioning_field=partitioning_field,
        clustering_fields=clustering_fields,
    )

    return content


# Example usage
if __name__ == "__main__":
    bigquery_table = "bef-cake-sandbox.qa_cake_recaptcha.recaptcha_message"
    topic_schema_definition = 'syntax = "proto3";\nmessage Recaptcha {\nstring id = 1; \nstring platform = 2; \nstring user_action = 3; \nstring token_action = 4; \nfloat score = 5;\nstring client_id = 6;\nstring device_id = 7;\nstring phone = 8;\nAssessment assessment = 9; \nmap<string, string> additional = 10;\nstring created_at = 11;//TIMESTAMP\n}\nmessage Assessment {\nRiskAnalysis risk_analysis = 1;\nTokenProperties token_prototypes = 2;\n}\nmessage RiskAnalysis {\nrepeated string reasons = 1;\n}\nmessage TokenProperties {\nstring invalid_reasons = 1;\nbool valid = 2; \n}'

    content = render_google_bigquery_table(
        resource_id=bigquery_table,
        protobuf_schema=topic_schema_definition,
        partitioning_field="created_at",
        clustering_fields=["client_id"],
    )

    with open(CWD / "test" / "test.tf", "w") as f:
        f.write(content)

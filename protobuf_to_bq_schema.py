import argparse
import json
import subprocess
from pathlib import Path
from string import Template

import jinja2
import yaml
from proto_schema_parser.ast import Import, Package
from proto_schema_parser.generator import Generator
from proto_schema_parser.parser import Parser


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
    lines = [
        line.split("//")
        for line in topic_schema_definition.split("\n")
        if not line.lower().startswith("reserved")  # Skip reserved fields
    ]
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


def render_bigquery_dataset(
    template_variables: dict,
) -> str:
    # Render the Terraform template
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"))
    template = environment.get_template("bigquery_dataset.yaml.jinja")
    content = template.render(
        project_id=template_variables["project_id"],
        dataset_id=template_variables["dataset_id"],
    )

    return content


def render_bigquery_table(
    template_variables: dict,
) -> str:
    # Render the Terraform template
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"))
    template = environment.get_template("bigquery_table.yaml.jinja")
    content = template.render(
        project_id=template_variables["project_id"],
        dataset_id=template_variables["dataset_id"],
        table_id=template_variables["table_id"],
        schema=template_variables["schema"],
        partitioning_field=template_variables["partitioning_field"],
    )

    return content


def search_yaml_files(directory: Path, pubsubschema_name: str):
    definitions = []
    for extension in ["*.yml", "*.yaml"]:
        for file_path in directory.rglob(extension):
            try:
                with file_path.open("r") as stream:
                    documents = yaml.safe_load_all(stream)
                    for doc in documents:
                        if isinstance(doc, dict) and (
                            doc.get("kind") == "PubSubSchema"
                            and doc.get("metadata", {}).get("name") == pubsubschema_name
                        ):
                            print(f"Match found in file: {file_path}")
                            spec_definition = doc.get("spec", {}).get("definition")
                            if spec_definition:
                                definitions.append(spec_definition)
                            else:
                                print("spec.definition not found in the document.")
            except yaml.YAMLError as exc:
                print(f"Error parsing YAML file {file_path}: {exc}")

    return definitions


def main():
    parser = argparse.ArgumentParser(
        description="Generate BigQuery schema and templates from PubSub schema."
    )
    parser.add_argument(
        "directory", type=Path, help="Directory to search for YAML files."
    )
    parser.add_argument(
        "pubsubschema_name", type=str, help="Name of the PubSub schema."
    )
    parser.add_argument("bigquery_table", type=str, help="BigQuery table resource ID.")
    parser.add_argument(
        "--partitioning_field",
        type=str,
        default="created_at",
        help="Partitioning field for BigQuery table.",
    )
    parser.add_argument(
        "--output_dir",
        type=Path,
        default=Path("target"),
        help="Output directory for generated files.",
    )

    args = parser.parse_args()

    topic_schema_definition = search_yaml_files(args.directory, args.pubsubschema_name)[
        0
    ]
    template_vars = generate_template_variables(
        resource_id=args.bigquery_table,
        protobuf_schema=topic_schema_definition,
        partitioning_field=args.partitioning_field,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    with open(args.output_dir / "dataset.yaml", "w") as f:
        f.write(render_bigquery_dataset(template_vars))
    with open(args.output_dir / "table.yaml", "w") as f:
        f.write(render_bigquery_table(template_vars))


if __name__ == "__main__":
    main()

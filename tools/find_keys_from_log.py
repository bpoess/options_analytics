#!/usr/bin/env python3
"""Parse JSON objects from log lines and report which key paths are required vs optional."""

import argparse
import json
import re
import sys
from pathlib import Path


def extract_dicts(log_path: Path, pattern: str) -> list[dict]:
    results = []
    with open(log_path) as f:
        for line_no, line in enumerate(f, 1):
            idx = line.find(pattern)
            if idx == -1:
                continue
            json_str = line[idx + len(pattern) :].strip()
            try:
                results.append(json.loads(json_str))
            except json.JSONDecodeError as e:
                print(
                    f"Warning: failed to parse JSON on line {line_no}: {e}",
                    file=sys.stderr,
                )
                pass

    return results


def collect_key_paths(d: dict, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    for key, value in d.items():
        path = f"{prefix}{key}"
        paths.add(path)
        if isinstance(value, dict):
            paths.update(collect_key_paths(value, f"{path}."))
    return paths


def to_snake_case(name: str) -> str:
    """Convert camelCase/PascalCase to snake_case."""
    # Insert underscore before uppercase letters that follow lowercase letters or digits
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert underscore between consecutive uppercase letters followed by lowercase
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


def to_pascal_case(name: str) -> str:
    """Convert a camelCase or snake_case name to PascalCase."""
    snake = to_snake_case(name)
    return "".join(word.capitalize() for word in snake.split("_"))


def infer_type(values: list) -> str:
    """Infer proto3 type from a list of observed Python values."""
    types_seen: set[type] = set()
    for v in values:
        if v is None:
            continue
        types_seen.add(type(v))

    if not types_seen:
        return "string"

    if types_seen == {bool}:
        return "bool"
    if types_seen == {int}:
        return "int64"
    if types_seen <= {int, float}:
        return "google.type.Decimal"
    if types_seen == {float}:
        return "google.type.Decimal"
    if types_seen == {str}:
        return "string"
    if types_seen == {dict}:
        return "message"
    if types_seen == {list}:
        return "list"

    return "string"


def infer_list_element_type(lists: list[list]) -> str:
    """Infer the proto3 type of elements in a repeated field."""
    all_elements = []
    for lst in lists:
        all_elements.extend(lst)
    if not all_elements:
        return "string"
    return infer_type(all_elements)


def build_message_tree(
    dicts: list[dict],
) -> dict:
    """Build a tree of field info from parsed dicts.

    Returns a dict mapping field names to:
      {"values": [...], "count": int, "children_dicts": [...]}
    """
    fields: dict[str, dict] = {}
    for d in dicts:
        for key, value in d.items():
            if key not in fields:
                fields[key] = {"values": [], "count": 0, "children_dicts": []}
            fields[key]["values"].append(value)
            fields[key]["count"] += 1
            if isinstance(value, dict):
                fields[key]["children_dicts"].append(value)
    return fields


def generate_proto_messages(
    dicts: list[dict], root_name: str
) -> tuple[list[str], bool]:
    """Generate proto3 message definitions.

    Returns (lines, uses_decimal).
    """
    all_messages: list[tuple[str, list[str]]] = []
    uses_decimal = False

    def process_message(name: str, message_dicts: list[dict]) -> None:
        nonlocal uses_decimal
        fields = build_message_tree(message_dicts)
        total = len(message_dicts)

        # Separate required and optional, sort alphabetically within each group
        required_fields = sorted(
            [(k, v) for k, v in fields.items() if v["count"] == total],
            key=lambda x: to_snake_case(x[0]),
        )
        optional_fields = sorted(
            [(k, v) for k, v in fields.items() if v["count"] < total],
            key=lambda x: to_snake_case(x[0]),
        )

        # Reserve our spot in the output order (root first, then children)
        msg_index = len(all_messages)
        all_messages.append((name, []))

        lines: list[str] = []
        field_num = 0

        for field_list, is_optional in [
            (required_fields, False),
            (optional_fields, True),
        ]:
            for key, info in field_list:
                field_num += 1
                snake_name = to_snake_case(key)
                proto_type = infer_type(info["values"])
                prefix = "optional " if is_optional else ""

                if proto_type == "message":
                    sub_name = to_pascal_case(key)
                    process_message(sub_name, info["children_dicts"])
                    lines.append(f"  {prefix}{sub_name} {snake_name} = {field_num};")
                elif proto_type == "list":
                    lists = [v for v in info["values"] if isinstance(v, list)]
                    elem_type = infer_list_element_type(lists)
                    if elem_type == "message":
                        sub_name = to_pascal_case(key)
                        all_elements = []
                        for lst in lists:
                            all_elements.extend(e for e in lst if isinstance(e, dict))
                        if all_elements:
                            process_message(sub_name, all_elements)
                        elem_type = sub_name
                    if elem_type == "google.type.Decimal":
                        uses_decimal = True
                    lines.append(f"  repeated {elem_type} {snake_name} = {field_num};")
                else:
                    if proto_type == "google.type.Decimal":
                        uses_decimal = True
                    lines.append(f"  {prefix}{proto_type} {snake_name} = {field_num};")

        all_messages[msg_index] = (name, lines)

    process_message(root_name, dicts)

    output: list[str] = []
    for msg_name, msg_lines in all_messages:
        if output:
            output.append("")
        output.append(f"message {msg_name} {{")
        output.extend(msg_lines)
        output.append("}")

    return output, uses_decimal


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_file", type=Path, help="Path to the log file")
    parser.add_argument(
        "pattern",
        help="Marker string preceding the JSON (e.g. 'Raw Position JSON: ')",
    )
    parser.add_argument(
        "--proto",
        nargs=2,
        metavar=("MESSAGE_NAME", "OUTPUT_FILE"),
        help="Generate proto3 schema with this root message name and write to file",
    )
    args = parser.parse_args()

    dicts = extract_dicts(args.log_file, args.pattern)
    if not dicts:
        print(f"No lines matching pattern {args.pattern!r} found", file=sys.stderr)
        sys.exit(1)

    if args.proto:
        message_name, output_file = args.proto
        message_lines, uses_decimal = generate_proto_messages(dicts, message_name)

        header = ['syntax = "proto3";', ""]
        if uses_decimal:
            header.append('import "google/type/decimal.proto";')
            header.append("")

        output_path = Path(output_file)
        output_path.write_text("\n".join(header + message_lines) + "\n")
        print(f"Wrote proto3 schema to {output_path}")
    else:
        total = len(dicts)
        path_counts: dict[str, int] = {}
        for d in dicts:
            for path in collect_key_paths(d):
                path_counts[path] = path_counts.get(path, 0) + 1

        required = sorted(p for p, c in path_counts.items() if c == total)
        optional = sorted((p, c) for p, c in path_counts.items() if c < total)

        print(f"Parsed {total} entries\n")

        print("Required:")
        for path in required:
            print(f"  {path}")

        if optional:
            print("\nOptional:")
            for path, count in optional:
                print(f"  {path}  ({count}/{total})")


if __name__ == "__main__":
    main()

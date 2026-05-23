#!/usr/bin/env python3
"""Kafka schema → C++ encoder/decoder generator.

Phase EX-3 proof-of-concept. Parses upstream Apache Kafka JSON message
schemas (`clients/src/main/resources/common/message/*.json`) and emits
header + source pairs that match Kawasan's hand-written encoder layout
byte-for-byte.

Scope of this PoC:
  - Primitive types: int8, int16, int32, int64, string
  - Compact (KIP-482) variants: COMPACT_STRING and COMPACT_ARRAY of struct
  - Version gating: "N+" and "N-M" expressions on fields
  - Tagged-fields placeholder (KIP-482) — emits a writeEmptyTaggedFields()
    call at the end of each flexible-version struct, matching Kawasan's
    current hand-written behavior.
  - Top-level structs and one level of nested struct (sufficient for
    ApiVersionsResponse's nested ApiVersion entries).

Out of scope (future iterations):
  - Tagged-fields with content
  - records, bytes, uuid, float64 types
  - Deeper struct nesting
  - DLM/derived field types

Equivalence is locked by tests/unit/codegen_golden_bytes_test.cpp,
which serializes hand-written and generated encoders against the same
payload and asserts byte-for-byte equality.

Usage:
    python3 tools/codegen/generate.py \
        --schema tools/codegen/schemas/ApiVersionsRequest.json \
        --out-dir include/kawasan/protocol/generated
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


# --------------------------------------------------------------------- #
# Version expression parsing
# --------------------------------------------------------------------- #

@dataclass(frozen=True)
class VersionRange:
    """Inclusive [lo, hi] range.

    hi=None means open-ended (e.g. '3+').
    A range with hi < lo is intentionally empty — used to represent
    KIP-152 APIs (SaslHandshake) whose `flexibleVersions: "none"` means
    no version ever gets the flex/compact treatment.
    """
    lo: int
    hi: int | None

    @property
    def is_empty(self) -> bool:
        return self.hi is not None and self.hi < self.lo

    def contains(self, version: int) -> bool:
        if self.is_empty:
            return False
        if self.hi is None:
            return version >= self.lo
        return self.lo <= version <= self.hi

    @staticmethod
    def parse(expr: str) -> "VersionRange":
        expr = expr.strip()
        # KIP-152 quirk: SaslHandshake (and a handful of older request
        # headers) stay non-flexible at every version. Upstream encodes
        # this as the literal "none" string. We represent it as an
        # empty range so every flex-guard `if` collapses to `if (false)`,
        # which the compiler then DCE's — producing the same bytes as
        # the hand-written non-flex path.
        if expr == "none":
            return VersionRange(lo=1, hi=0)
        if expr.endswith("+"):
            return VersionRange(lo=int(expr[:-1]), hi=None)
        if "-" in expr:
            lo, hi = expr.split("-")
            return VersionRange(lo=int(lo), hi=int(hi))
        v = int(expr)
        return VersionRange(lo=v, hi=v)

    def emit_guard(self, var: str = "api_version") -> str:
        """Generate a C++ if-condition expression for this range.

        Returns the literal "false" for empty ranges so the generated
        if-block becomes statically dead and the compiler eliminates it.
        """
        if self.is_empty:
            return "false"
        if self.hi is None:
            return f"{var} >= {self.lo}"
        if self.lo == self.hi:
            return f"{var} == {self.lo}"
        return f"({var} >= {self.lo} && {var} <= {self.hi})"


# --------------------------------------------------------------------- #
# Type model
# --------------------------------------------------------------------- #

PRIMITIVE_KINDS = {
    "int8":  ("int8_t",  "writeInt8",  "readInt8"),
    "int16": ("int16_t", "writeInt16", "readInt16"),
    "int32": ("int32_t", "writeInt32", "readInt32"),
    "int64": ("int64_t", "writeInt64", "readInt64"),
}


@dataclass
class Field:
    name: str
    type_expr: str          # e.g. "int16", "string", "[]ApiVersion"
    versions: VersionRange
    flexible: VersionRange | None  # range over which the COMPACT variants apply
    ignorable: bool = False
    nested: "Struct | None" = None  # set if type is []SomeStruct

    @property
    def cpp_member(self) -> str:
        return f"{snake(self.name)}_"

    @property
    def is_array(self) -> bool:
        return self.type_expr.startswith("[]")


@dataclass
class Struct:
    name: str
    fields: list[Field] = field(default_factory=list)


@dataclass
class Schema:
    name: str            # e.g. "ApiVersionsRequest"
    api_key: int
    kind: str            # "request" | "response"
    valid_versions: VersionRange
    flexible_versions: VersionRange
    top: Struct          # top-level struct (== name)
    nested_structs: dict[str, Struct] = field(default_factory=dict)


def snake(camel: str) -> str:
    """ApiVersions → api_versions, ClientSoftwareName → client_software_name."""
    out = re.sub(r"(?<!^)(?=[A-Z])", "_", camel)
    return out.lower()


# --------------------------------------------------------------------- #
# Schema loader
# --------------------------------------------------------------------- #

def load_schema(path: Path) -> Schema:
    with path.open() as fh:
        raw = json.load(fh)
    valid = VersionRange.parse(raw["validVersions"])
    flex = VersionRange.parse(raw["flexibleVersions"])

    top = Struct(name=raw["name"])
    nested: dict[str, Struct] = {}

    def parse_field(f: dict) -> Field:
        type_expr = f["type"]
        ver = VersionRange.parse(f["versions"])
        nested_struct: Struct | None = None
        if type_expr.startswith("[]"):
            elem = type_expr[2:]
            if elem not in PRIMITIVE_KINDS and elem != "string":
                # It's a nested struct.
                inline_fields = f.get("fields", [])
                if not inline_fields:
                    raise ValueError(
                        f"nested struct '{elem}' on field '{f['name']}' "
                        f"has no inline 'fields' (cross-file refs not supported in PoC)")
                nested_struct = Struct(name=elem,
                                       fields=[parse_field(x) for x in inline_fields])
                nested[elem] = nested_struct
        return Field(
            name=f["name"], type_expr=type_expr, versions=ver,
            flexible=flex, ignorable=f.get("ignorable", False),
            nested=nested_struct,
        )

    for f in raw["fields"]:
        top.fields.append(parse_field(f))

    return Schema(name=raw["name"], api_key=raw["apiKey"], kind=raw["type"],
                  valid_versions=valid, flexible_versions=flex,
                  top=top, nested_structs=nested)


# --------------------------------------------------------------------- #
# C++ emitter
# --------------------------------------------------------------------- #

def cpp_type(field: Field) -> str:
    if field.type_expr in PRIMITIVE_KINDS:
        return PRIMITIVE_KINDS[field.type_expr][0]
    if field.type_expr == "string":
        return "std::string"
    if field.type_expr.startswith("[]"):
        elem = field.type_expr[2:]
        if elem in PRIMITIVE_KINDS:
            return f"std::vector<{PRIMITIVE_KINDS[elem][0]}>"
        if elem == "string":
            return "std::vector<std::string>"
        # nested struct
        return f"std::vector<{elem}>"
    raise ValueError(f"unsupported type {field.type_expr}")


def emit_nested_struct(struct: Struct, lines: list[str]) -> None:
    lines.append(f"    struct {struct.name} {{")
    for f in struct.fields:
        lines.append(f"        {cpp_type(f)} {f.cpp_member}{{}};")
    lines.append("    };")
    lines.append("")


def emit_field_member(field: Field, lines: list[str]) -> None:
    cpp = cpp_type(field)
    lines.append(f"    {cpp} {field.cpp_member}{{}};")


def emit_encode_field(field: Field, lines: list[str], indent: str,
                      flex_range: VersionRange) -> None:
    ver_guard = field.versions.emit_guard()
    if field.type_expr in PRIMITIVE_KINDS:
        _cpp, write_fn, _r = PRIMITIVE_KINDS[field.type_expr]
        lines.append(f"{indent}if ({ver_guard}) {{")
        lines.append(f"{indent}    buffer.{write_fn}({field.cpp_member});")
        lines.append(f"{indent}}}")
    elif field.type_expr == "string":
        lines.append(f"{indent}if ({ver_guard}) {{")
        lines.append(f"{indent}    if ({flex_range.emit_guard()}) {{")
        lines.append(f"{indent}        buffer.writeCompactString({field.cpp_member});")
        lines.append(f"{indent}    }} else {{")
        lines.append(f"{indent}        buffer.writeString({field.cpp_member});")
        lines.append(f"{indent}    }}")
        lines.append(f"{indent}}}")
    elif field.type_expr.startswith("[]"):
        elem = field.type_expr[2:]
        lines.append(f"{indent}if ({ver_guard}) {{")
        lines.append(f"{indent}    if ({flex_range.emit_guard()}) {{")
        lines.append(f"{indent}        buffer.writeCompactArrayLen(static_cast<int32_t>({field.cpp_member}.size()));")
        lines.append(f"{indent}    }} else {{")
        lines.append(f"{indent}        buffer.writeInt32(static_cast<int32_t>({field.cpp_member}.size()));")
        lines.append(f"{indent}    }}")
        lines.append(f"{indent}    for (const auto& item : {field.cpp_member}) {{")
        if elem in PRIMITIVE_KINDS:
            wf = PRIMITIVE_KINDS[elem][1]
            lines.append(f"{indent}        buffer.{wf}(item);")
        else:
            # nested struct
            for sub in field.nested.fields:
                sub_guard = sub.versions.emit_guard()
                if sub.type_expr in PRIMITIVE_KINDS:
                    wf = PRIMITIVE_KINDS[sub.type_expr][1]
                    lines.append(f"{indent}        if ({sub_guard}) {{")
                    lines.append(f"{indent}            buffer.{wf}(item.{sub.cpp_member});")
                    lines.append(f"{indent}        }}")
                else:
                    raise ValueError(f"nested-struct field type {sub.type_expr} not supported in PoC")
            lines.append(f"{indent}        if ({flex_range.emit_guard()}) {{")
            lines.append(f"{indent}            buffer.writeUnsignedVarInt(0);  // empty tagged-fields per nested entry")
            lines.append(f"{indent}        }}")
        lines.append(f"{indent}    }}")
        lines.append(f"{indent}}}")
    else:
        raise ValueError(f"unsupported type {field.type_expr}")


def emit_decode_field(field: Field, lines: list[str], indent: str,
                      flex_range: VersionRange) -> None:
    ver_guard = field.versions.emit_guard()
    if field.type_expr in PRIMITIVE_KINDS:
        _cpp, _w, read_fn = PRIMITIVE_KINDS[field.type_expr]
        lines.append(f"{indent}if ({ver_guard}) {{")
        lines.append(f"{indent}    {field.cpp_member} = buffer.{read_fn}();")
        lines.append(f"{indent}}}")
    elif field.type_expr == "string":
        lines.append(f"{indent}if ({ver_guard}) {{")
        lines.append(f"{indent}    if ({flex_range.emit_guard()}) {{")
        lines.append(f"{indent}        {field.cpp_member} = buffer.readCompactString();")
        lines.append(f"{indent}    }} else {{")
        lines.append(f"{indent}        {field.cpp_member} = buffer.readString();")
        lines.append(f"{indent}    }}")
        lines.append(f"{indent}}}")
    elif field.type_expr.startswith("[]"):
        elem = field.type_expr[2:]
        lines.append(f"{indent}if ({ver_guard}) {{")
        lines.append(f"{indent}    int32_t len = 0;")
        lines.append(f"{indent}    if ({flex_range.emit_guard()}) {{")
        lines.append(f"{indent}        len = buffer.readCompactArrayLen();")
        lines.append(f"{indent}    }} else {{")
        lines.append(f"{indent}        len = buffer.readInt32();")
        lines.append(f"{indent}    }}")
        lines.append(f"{indent}    if (len < 0) len = 0;")
        lines.append(f"{indent}    {field.cpp_member}.resize(len);")
        lines.append(f"{indent}    for (int32_t i = 0; i < len; ++i) {{")
        if elem in PRIMITIVE_KINDS:
            rf = PRIMITIVE_KINDS[elem][2]
            lines.append(f"{indent}        {field.cpp_member}[i] = buffer.{rf}();")
        else:
            for sub in field.nested.fields:
                sub_guard = sub.versions.emit_guard()
                if sub.type_expr in PRIMITIVE_KINDS:
                    rf = PRIMITIVE_KINDS[sub.type_expr][2]
                    lines.append(f"{indent}        if ({sub_guard}) {{")
                    lines.append(f"{indent}            {field.cpp_member}[i].{sub.cpp_member} = buffer.{rf}();")
                    lines.append(f"{indent}        }}")
                else:
                    raise ValueError(f"nested-struct field type {sub.type_expr} not supported in PoC")
            lines.append(f"{indent}        if ({flex_range.emit_guard()}) {{")
            lines.append(f"{indent}            buffer.skipTaggedFields();")
            lines.append(f"{indent}        }}")
        lines.append(f"{indent}    }}")
        lines.append(f"{indent}}}")
    else:
        raise ValueError(f"unsupported type {field.type_expr}")


def generate(schema: Schema) -> tuple[str, str]:
    """Generate (header, source) pair as strings."""
    header_lines: list[str] = []
    header_lines.append("// Auto-generated by tools/codegen/generate.py — DO NOT EDIT.")
    header_lines.append(f"// Source schema: {schema.name}.json (apiKey={schema.api_key}, {schema.kind})")
    header_lines.append(f"// Valid versions: [{schema.valid_versions.lo}..{schema.valid_versions.hi or '∞'}]")
    header_lines.append(f"// Flexible: from v{schema.flexible_versions.lo}+")
    header_lines.append("#pragma once")
    header_lines.append("")
    header_lines.append("#include <cstdint>")
    header_lines.append("#include <string>")
    header_lines.append("#include <vector>")
    header_lines.append("#include \"kawasan/common/buffer.h\"")
    header_lines.append("")
    header_lines.append("namespace kawasan::protocol::generated {")
    header_lines.append("")
    header_lines.append(f"class {schema.name} {{")
    header_lines.append(" public:")
    for nested in schema.nested_structs.values():
        emit_nested_struct(nested, header_lines)
    header_lines.append("    void encode(kawasan::Buffer& buffer, int16_t api_version) const;")
    header_lines.append("    void decode(kawasan::Buffer& buffer, int16_t api_version);")
    header_lines.append("")
    for f in schema.top.fields:
        emit_field_member(f, header_lines)
    header_lines.append("};")
    header_lines.append("")
    header_lines.append("}  // namespace kawasan::protocol::generated")
    header_lines.append("")

    source_lines: list[str] = []
    source_lines.append("// Auto-generated by tools/codegen/generate.py — DO NOT EDIT.")
    source_lines.append(f"#include \"kawasan/protocol/generated/{schema.name}.h\"")
    source_lines.append("")
    source_lines.append("namespace kawasan::protocol::generated {")
    source_lines.append("")
    source_lines.append(f"void {schema.name}::encode(kawasan::Buffer& buffer, int16_t api_version) const {{")
    for f in schema.top.fields:
        emit_encode_field(f, source_lines, "    ", schema.flexible_versions)
    # Top-level tagged-fields suffix for flexible versions.
    source_lines.append(f"    if ({schema.flexible_versions.emit_guard()}) {{")
    source_lines.append("        buffer.writeUnsignedVarInt(0);  // top-level empty tagged fields")
    source_lines.append("    }")
    source_lines.append("}")
    source_lines.append("")
    source_lines.append(f"void {schema.name}::decode(kawasan::Buffer& buffer, int16_t api_version) {{")
    for f in schema.top.fields:
        emit_decode_field(f, source_lines, "    ", schema.flexible_versions)
    source_lines.append(f"    if ({schema.flexible_versions.emit_guard()}) {{")
    source_lines.append("        buffer.skipTaggedFields();")
    source_lines.append("    }")
    source_lines.append("}")
    source_lines.append("")
    source_lines.append("}  // namespace kawasan::protocol::generated")
    source_lines.append("")
    return "\n".join(header_lines), "\n".join(source_lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", required=True, help="Path to schema JSON")
    ap.add_argument("--out-dir", required=True,
                    help="Output dir; will write to <out-dir>/<Name>.h and <Name>.cpp")
    args = ap.parse_args()

    schema_path = Path(args.schema)
    schema = load_schema(schema_path)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    header, source = generate(schema)
    (out_dir / f"{schema.name}.h").write_text(header)
    (out_dir / f"{schema.name}.cpp").write_text(source)
    print(f"Generated {schema.name}.h and {schema.name}.cpp in {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

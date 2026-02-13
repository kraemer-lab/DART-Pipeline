import importlib
import importlib.metadata
import shutil
from typing import Dict, List

import pandas as pd
import streamlit as st
import tree_sitter_bash as tsbash
from pandas.io.formats.style import Styler
from streamlit.runtime.state import SessionStateProxy
from tree_sitter import Language, Node, Parser

from .types import ASTValueNode, PreReqInfo

prereq_py_libs = ["xarray", "rasterio", "cdsapi"]
prereq_execs = ["dart-bias-correct", "cdo", "curl"]


def check_lib(name: str) -> PreReqInfo:
    try:
        importlib.import_module(name)
        return PreReqInfo(
            "Python package",
            name,
            "Installed",
            f"version: {importlib.metadata.version(name)}",
        )
    except Exception as e:
        return PreReqInfo(
            "Python package",
            name,
            "Missing",
            str(e),
        )


def check_exec(name: str) -> PreReqInfo:
    path = shutil.which(name)
    return PreReqInfo(
        "CLI executable",
        name,
        "Installed" if (path is not None) else "Missing",
        f"which: {str(path)}",
    )


def prereqs_check() -> Styler:
    rows: List[PreReqInfo] = []

    for lib in prereq_py_libs:
        rows.append(check_lib(lib))

    for exec in prereq_execs:
        rows.append(check_exec(exec))

    df = pd.DataFrame(rows).style.apply(
        lambda r: [
            "background-color: lightgreen"
            if rr == "Installed"
            else "background-color: pink"
            for rr in r
        ],
        axis=0,
        subset=["Status"],
    )

    return df


BASH_PARSER = Parser(Language(tsbash.language()))


def extract_ast_vars(
    root: Node,
    source_bytes: bytes,
) -> Dict[str, ASTValueNode]:
    ast_vars: Dict = {}

    # recursively walk through the AST
    def walk(node: Node) -> None:
        if node.type == "variable_assignment":
            name_node = node.child_by_field_name("name")
            val_node = node.child_by_field_name("value")

            if name_node is not None and val_node is not None:
                key = source_bytes[name_node.start_byte : name_node.end_byte].decode(
                    "utf-8"
                )
                val = (
                    source_bytes[val_node.start_byte : val_node.end_byte]
                    .decode("utf-8")
                    .strip("\"'")
                )

                ast_vars[key] = ASTValueNode(
                    value=val,
                    start_byte=val_node.start_byte,
                    end_byte=val_node.end_byte,
                )

        for child in node.children:
            walk(child)

    walk(root)
    return ast_vars


def assign_ast_vars(
    source_bytes: bytes, old_vars: Dict[str, ASTValueNode], new_vars: Dict[str, str]
) -> bytes:
    config_bytes = bytearray(source_bytes)
    print("********************")

    print(f"{new_vars=}")
    for old_var in sorted(
        old_vars.items(), key=lambda var: var[1].start_byte, reverse=True
    ):
        old_key = old_var[0]
        old_astvalnode = old_var[1]

        print(f"{old_key=}")
        print(f"{old_astvalnode=}")

        if old_key not in new_vars.keys():
            continue

        start_b = old_astvalnode.start_byte
        end_b = old_astvalnode.end_byte

        old_val_bytes = source_bytes[start_b:end_b].decode("utf-8")
        new_val = new_vars[old_key]

        # preserve quoting if needed
        if old_val_bytes.startswith(("'", '"')) and old_val_bytes.endswith(("'", '"')):
            quote = old_val_bytes[0]
            new_val = f"{quote}{new_val}{quote}"

        config_bytes[start_b:end_b] = new_val.encode("utf-8")

    return bytes(config_bytes)


def print_current_config(session_state: SessionStateProxy):
    # render `config.sh` for reference
    try:
        config_pretty = st.session_state["config_pretty"]
        _ = st.session_state["config_vars"]
        st.subheader("Loaded configuration")
        st.write(config_pretty)
    except KeyError:
        st.subheader(":red[KeyError]")
        st.write(
            "This most likely means the config file isn't loaded correctly. Please go back to the **`1. Configuration`** page to re-read the config file"
        )
    except Exception as e:
        st.subheader(f":red-badge[{e}]")
        st.write("Uncaught exception")

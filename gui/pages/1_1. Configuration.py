import sys
from pathlib import Path
from typing import Dict

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from gui.types import ASTValueNode  # noqa: E402
from gui.utils import (  # noqa: E402
    BASH_PARSER,
    assign_ast_vars,
    extract_ast_vars,
)

CONFIG_SH_PATH = Path(__file__).parents[2] / "config.sh"


def read_config_sh() -> str:
    return CONFIG_SH_PATH.read_text()


def parse_config_sh(config_text: str) -> Dict[str, ASTValueNode]:
    config_bytes = bytes(config_text, "utf8")
    config_ast = BASH_PARSER.parse(config_bytes)

    return extract_ast_vars(config_ast.root_node, config_bytes)


def pretty_config_vars(ast_vars: Dict[str, ASTValueNode]):
    return {
        k: v.value
        for k, v in ast_vars.items()
        # dont return arithmetic vars
        if not (k.startswith("_") or v.value.startswith("$"))
    }


def reload_session() -> None:
    st.session_state["config_text"] = read_config_sh()
    st.session_state["config_vars"] = parse_config_sh(st.session_state["config_text"])
    st.session_state["config_pretty"] = pretty_config_vars(
        st.session_state["config_vars"]
    )
    st.session_state["config_version"] += 1


def collect_input_vars() -> Dict[str, str]:
    version = st.session_state["config_version"]
    out_d: Dict[str, str] = {}

    for key in st.session_state["config_pretty"].keys():
        widget_key = f"{version}_{key}"
        if widget_key in st.session_state:
            out_d[key] = st.session_state[widget_key]

    return out_d


def run():
    st.set_page_config(
        page_title="DART-Pipeline Configuration",
        page_icon="⚙️",
    )
    st.title("DART-Pipeline Configuration")
    st.write(
        "**Currently there is no input sanitisation**. Please be responsible and careful with your inputs or the pipeline won't run as expected"
    )

    # initial state
    if "config_version" not in st.session_state:
        st.session_state["config_version"] = 0
    if "config_vars" not in st.session_state:
        reload_session()

    # re-read and re-parse on demand
    if st.button("Re-read variables from `config.sh`"):
        reload_session()

    # render config form
    items = list(st.session_state["config_pretty"].items())

    for i in range(0, len(items), 2):
        col1, col2 = st.columns([1, 1])

        k1, v1 = items[i]
        with col1:
            st.text_input(
                k1,
                v1,
                key=f"{st.session_state['config_version']}_{k1}",
            )

        if i + 1 < len(items):
            k2, v2 = items[i + 1]
            with col2:
                st.text_input(
                    k2,
                    v2,
                    key=f"{st.session_state['config_version']}_{k2}",
                )

    # assign new values to config.sh
    if st.button("Export to `config.sh`"):
        old_bytes = bytes(st.session_state["config_text"], "utf8")
        new_bytes = assign_ast_vars(
            source_bytes=old_bytes,
            old_vars=st.session_state["config_vars"],
            new_vars=collect_input_vars(),
        )
        # backup previous file
        (Path(__file__).parents[2] / "config.sh.bkup").write_bytes(old_bytes)
        # write new config
        CONFIG_SH_PATH.write_bytes(new_bytes)
        # reload session
        reload_session()

    # render `config.sh` for reference
    st.write("### Actual `config.sh` contents:")
    st.code(st.session_state["config_text"])


run()

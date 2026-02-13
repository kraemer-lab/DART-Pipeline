import os
import subprocess
from subprocess import Popen

import streamlit as st
from streamlit.delta_generator import DeltaGenerator
from streamlit.runtime.state import SessionStateProxy

from gui.utils import print_current_config


def run_subproc(
    cmd_list: list[str], st_console: DeltaGenerator, st_session_state: SessionStateProxy
) -> Popen:
    cmd_list = ["stdbuf", "-oL", "-eL", *cmd_list]
    subproc = subprocess.Popen(
        cmd_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    st_session_state["bc_log"] += f"Started subprocess with pid {subproc.pid}\n"
    st_console.code(st_session_state["bc_log"], language="bash", height=300)

    for line in subproc.stdout:  # pyright: ignore[reportOptionalIterable]
        st_session_state["bc_log"] += line
        st_console.code(st_session_state["bc_log"], language="bash", height=300)

    subproc.wait()

    return subproc


def run():
    st.set_page_config(
        page_title="DART-Pipeline Bias correction",
        page_icon="⚙️",
    )
    st.title("DART-Pipeline Bias correction")
    st.write(
        "Applies bias correction to precipitation data using the `dart-bias-correct` tool. This process adjusts modeled precipitation values to better match historical observations."
    )

    #
    #############
    print_current_config(st.session_state)
    config_vars = st.session_state["config_vars"]
    fetch_start = int(config_vars["START_YEAR"].value) - 1
    fetch_end = int(config_vars["END_YEAR"].value) + 1
    BC_PRECIP_REF = config_vars["BC_PRECIP_REF"].value
    BC_HISTORICAL_OBS = config_vars["BC_HISTORICAL_OBS"].value

    #
    #############
    st.subheader("Extra configuration")

    if int(config_vars["BC_ENABLE"].value) != 1:
        st.write("""
        :red[Bias correction is turned off] (`BC_ENABLE` ≠ 1)\n
        - If this is expected, you can skip this step and go to `4. Processing data`\n
        - If this is not expected, please set BC_ENABLE to 1 in step `1. Configuration` first
        """)

    #
    #############
    st.write("#### Clip percentile")
    BC_CLIP_PRECIP_PERCENTILE = st.number_input(
        label="Percentile at which to clip reference precipitation dataset (default: 0.99)",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.99,
    )

    #
    #############
    st.write("#### Reference dataset")
    if os.path.exists(BC_PRECIP_REF):
        st.write(":green[Reference dataset found] ✅")
    else:
        st.write("""
        :red[The precipitation reference dataset (`BC_PRECIP_REF`) is not found, cannot perform bias correction]\n
        - Please double-check `BC_PRECIP_REF` in step `1. Configuration`
        """)

    #
    #############
    st.write("#### Historical observation dataset")
    if os.path.exists(BC_HISTORICAL_OBS):
        st.write(":green[Historical observation dataset found] ✅")
    else:
        st.write("""
        :red[The historical observation dataset (`BC_HISTORICAL_OBS`) is not found, cannot perform bias correction]\n
        - Please double-check `BC_HISTORICAL_OBS` in step `1. Configuration`
        """)

    #
    #############
    st.subheader("Run bias correction")
    run_bc = st.button("Click to run bias correction")

    ISO3 = config_vars["ISO3"].value

    if "bc_log" not in st.session_state:
        st.session_state["bc_log"] = ""

    st_console = st.code(st.session_state["bc_log"], language="bash", height=300)

    if run_bc:
        for year in range(fetch_start, fetch_end + 1, 1):
            st.session_state["bc_log"] = ""
            subproc = run_subproc(
                [
                    "dart-bias-correct",
                    "precipitation",
                    BC_PRECIP_REF,
                    BC_HISTORICAL_OBS,
                    f"{ISO3}-{year}",
                    f"--clip-precip-percentile={BC_CLIP_PRECIP_PERCENTILE}",
                ],
                st_console,
            )

            if subproc.returncode == 0:
                st.write(f"Bias correction for {year=} finished without error")
            else:
                st.write(
                    f"Bias correction for {year=} killed with {subproc.returncode=}"
                )


run()

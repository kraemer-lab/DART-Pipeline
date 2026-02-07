import sys
from pathlib import Path

import streamlit as st
from streamlit.logger import get_logger

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from gui.utils import prereqs_check  # noqa: E402

LOGGER = get_logger(__name__)
prereqs_libs_df = prereqs_check()


def run():
    st.set_page_config(
        page_title="DART-Pipeline GUI",
        page_icon="⚙️",
    )

    st.title("DART-Pipeline GUI")
    st.caption("-- made with [Streamlit](https://streamlit.io/)")

    st.subheader("README")
    st.write(
        "Please read [the official documentation](https://dart-pipeline.readthedocs.io/en/latest/index.html)"
    )
    st.write(
        "This GUI mimics the workflow outlined in the official doc above, provide a visual interface and thin wrapper around the scripts"
    )

    st.subheader("Pipeline prerequisites")
    st.write(
        "You should have all the prerequisite Python libraries and CLI executables below for DART-Pipeline to work as expected"
    )
    st.dataframe(prereqs_libs_df, hide_index=True)


if __name__ == "__main__":
    run()

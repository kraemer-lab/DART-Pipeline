import nox

nox.options.default_venv_backend = "uv"

PYTHON_VERSIONS = "3.11"


@nox.session
def lint(session):
    session.install("ruff")
    session.run("ruff", "check", "src")
    session.run("ruff", "check", "tests")


@nox.session(python=PYTHON_VERSIONS)
def tests(session):
    session.env.update({"UV_PROJECT_ENVIRONMENT": session.virtualenv.location})
    session.run("uv", "sync", "--all-extras")
    session.run("uv", "run", "pytest", "-n", "auto", "-vv", "--cov")


@nox.session(python="3.10", default=False)
def docs(session):
    session.env.update({"UV_PROJECT_ENVIRONMENT": session.virtualenv.location})
    session.run("uv", "sync", "--all-extras")
    # fmt: off
    session.run(
        "uv", "run", "-m", "sphinx", "-T", "-b", "html",
        "-d", "docs/_build/doctrees", "-D",
        "language=en", "docs", "html"
    )
    # fmt: on

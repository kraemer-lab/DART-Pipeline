# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import pathlib
import sys
sys.path.insert(-1, pathlib.Path(__file__).parents[2].resolve().as_posix())
path = pathlib.Path('../../DART-Pipeline').resolve().as_posix()
sys.path.insert(-1, path)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'DART Pipeline'
copyright = '2023, Rowan Nicholls and Prathyush Sambaturu'
author = 'Rowan Nicholls and Prathyush Sambaturu'
release = 'v0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ['_templates']
exclude_patterns = []
latex_elements = {
    'papersize': 'a4paper',
    'extraclassoptions': 'openany',
    'preamble': r'''
'''
}
today_fmt = '%Y-%m-%d'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']

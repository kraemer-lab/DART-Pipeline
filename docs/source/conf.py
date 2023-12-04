"""
Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html
"""
import pathlib
import sys

path = pathlib.Path(__file__).parents[2].resolve().as_posix()
sys.path.insert(0, path)
path = pathlib.Path(path, 'A Collate Data').resolve().as_posix()
sys.path.insert(0, path)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'DART Pipeline'
copyright = '2023, Rowan Nicholls and Prathyush Sambaturu'
author = 'Rowan Nicholls and Prathyush Sambaturu'
release = 'v0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon'
]

templates_path = ['_templates']
exclude_patterns = []
latex_elements = {
    'papersize': 'a4paper',
    'extraclassoptions': 'openany',
    'preamble': r'''
\usepackage{pmboxdraw} % Needed for box-drawing characters
\usepackage{tikz} %draw figures, flowcharts, etc
'''
}
today_fmt = '%Y-%m-%d'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']

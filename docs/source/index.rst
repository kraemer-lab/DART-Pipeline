.. DART Pipeline documentation master file, created by
   sphinx-quickstart on Tue Oct 31 14:29:29 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::

.. include:: ../../README.rst

Usage
=====
This section details the usage of all scripts, modules and functions in the pipeline.

A Collate Data
--------------
The "A Collate Data" folder contains one script - `collate_data.py` - which is documented here:

collate_data.py
^^^^^^^^^^^^^^^

.. automodule:: collate_data

____

.. autofunction:: collate_data.get_base_directory

____

.. autofunction:: collate_data.get_password

____

.. autofunction:: collate_data.walk

____

.. autofunction:: collate_data.download_worldpop_data

____

.. autofunction:: collate_data.download_gadm_data

____

B Process Data
--------------
The "B Process Data" folder contains one script - `process_data.py` - which
processes data that has already been downloaded and collated:

process_data.py
^^^^^^^^^^^^^^^

.. automodule:: process_data

____

.. autofunction:: process_data.get_base_directory

____

.. autofunction:: process_data.plot_pop_density

____

.. autofunction:: process_data.pixel_to_latlon

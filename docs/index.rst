.. Marketflow documentation master file, created by
   sphinx-quickstart on Mon Apr 25 16:50:52 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Marketflow's documentation!
======================================

Contents:

.. toctree::
   :maxdepth: 2

Introduction
============

Marketflow is a collection of tools for reading and processing financial market
data, which often comes in difficult-to-work-with formats. See below for an
overview of the data sources currently supported. While the normative user is
expected to primarily use Python, use may be made of other runtimes, for
example we currently have some experiments with Spark (though we've not yet
included them here). If it seems useful to building a community around
open-source econometrics, etc. please consider contributing your code!

This library was intially developed in a BIDS collaborative project with the
D-Lab's director, Justin McCrary, and led by Dav Clark (a member of both D-Lab
and BIDS). It is the result of many contributors, all of whom are currently
attributed as authors.

TAQ
===

The Trade and Quote (TAQ) dataset from the NYSE comes in heavily compressed zip
archives of fixed-width data. The column formats have changed over the years,
though we currently believe that files of a given width will have the same
format (essentially, the format has only ever been enlarged to include more
columns). The documentation for these formats is usually hidden behind a
paywall, but can likely be obtained from wherever you are getting the data
themselves.

We have optimized a pathway that maps bytes from the file into a numpy
structured array, with some degree of control over conversion to binary types.
We feel that our fixed-width parser is of high quality and should likely be
split out into a separate library!

To perform various operations on the TAQ data (or other data in columnar
format), you may use the generators provided in the processing submodule (XXX
link).

Examples
========

The below can be used as a part of your analysis, or as informal documentation
illustrating how the library can be used.

Utility scripts
---------------

There are a handful of scripts installed along with this package:

taq2h5
  Runs the taq2h5 function from marketflow.hdf5, converting zipped fixed-width
  TAQ BBO files to HDF5 with PyTables. Invoke with -h or -help for more info.
pyitch
  Runs the main function from marketflow.ITCHbin, exporting the variable-length
  binary records from binary ITCH datafiles.

Jupyter Python Notebooks
------------------------

We have some examples of code in the `tests/` directory in the marketflow repo.
These can be browsed `directly on
GitHub <https://github.com/dlab-projects/marketflow/tree/master/tests>`_.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

# Ethdata
[![Build Status](https://travis-ci.org/blocklytics/eth-data-tools.svg?branch=master)](https://travis-ci.org/blocklytics/eth-data-tools)
[![codecov](https://codecov.io/gh/blocklytics/eth-data-tools/branch/master/graph/badge.svg)](https://codecov.io/gh/blocklytics/eth-data-tools)
[![PyPI version](https://badge.fury.io/py/eth-data-tools.svg)](https://badge.fury.io/py/eth-data-tools)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Offers developers and analysts a simple way to extract and analyse historical Ethereum data using free, public APIs and services.

 * Pull Ethereum data without waiting to sync a node
 * Data is automatically converted (as much as possible)
 * Start analysing data right away using Pandas or Excel

# Documentation

https://docs.blocklytics.org/ethdata/introduction

Note you will need set two environment variables:

- GOOGLE_APPLICATION_CREDENTIALS set to your json service account key. See [here](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries) for how to create this
- ETHERSCAN_API_KEY. Key can be created for free on https://etherscan.io

# GitHub

https://github.com/blocklytics/eth-data-tools

# How to install from local copy

- git clone locally
- conda create --name ethdata
- condo activate ethdata
- conda install pip
- which pip [just to check you're now referring to the pip in your conda env]
- pip install -e . [this installs the package into ethdata according to the setup.py instructions]
- conda install pytest-cov [required to run the tests]

# How to install from remote

This package is hosted here: https://pypi.org/project/eth-data-tools/

pip install eth-data-tools

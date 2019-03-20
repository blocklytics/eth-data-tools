import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="eth-data-tools",
    version="0.0.3",
    author="Blocklytics",
    author_email="hello@blocklytics.org",
    description="A Python package to help analyse Ethereum data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/blocklytics/eth-data-tools",
    packages=setuptools.find_packages(),
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English"
    ],
    install_requires=[
    	'requests',
    	'pycryptodome',
    	'google-cloud-bigquery[pyarrow]',
    	'pandas'
    ],
	tests_require=[
		'pytest'
	]
)
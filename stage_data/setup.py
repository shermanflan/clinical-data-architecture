import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="spark_etl",
    version="0.0.1",
    author="Sherman Flan",
    author_email="shermanflan@gmail.com",
    description="A set of library modules for PySpark tasks.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shermanflan/clinical-data-architecture/stage_data",
    # This indicates which packages to install.
    packages=['spark_etl'],  # Or dynamically via: setuptools.find_packages(),
    # This specifies which dependencies to install.
    install_requires=[
        'boto3>=1.16.7'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
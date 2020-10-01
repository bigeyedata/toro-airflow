import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="toro-airflow",  # Replace with your own username
    version="0.0.2",
    author="Toro Data Labs, Inc",
    author_email="support@torodata.io",
    description="Airflow operators to be used with Toro",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/torodata/toro-airflow",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

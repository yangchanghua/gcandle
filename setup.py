import setuptools
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gcandle", # Replace with your own username
    version="0.0.7",
    author="ych",
    author_email="yangchanghua@gmail.com",
    description="A library for quant trade",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yangchanghua/gcandle",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "pandas",
        "numpy",
        "pymongo",
        "matplotlib",
        "pytdx"
    ],
)

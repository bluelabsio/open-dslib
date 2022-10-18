import setuptools
import getpass

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="open-dslib",
    version="0.0.1",
    description="Open Source BlueLabs data science library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bluelabsio/open-dslib",
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
    install_requires=[
        'pandas',
        'numpy',
        'matplotlib',
        'typing',
        'sqlalchemy',
    ]
)

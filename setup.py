from setuptools import setup, find_packages

setup(
    name="gfw_pixetl",
    version="0.3.1",
    description="Tool to preprocess GFW tiles",
    packages=find_packages(exclude=("tests",)),
    author="Thomas Maschler",
    license="MIT",
    entry_points="""
            [console_scripts]
            pixetl=gfw_pixetl.pixetl:cli
            pixetl_prep=gfw_pixetl.source_prep:cli
            """,
)

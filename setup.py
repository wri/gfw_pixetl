from setuptools import setup

setup(
    name="gfw_tile_prep",
    version="0.1.0",
    description="Tool to preprocess GFW tiles",
    packages=["gfw_tile_prep"],
    author="Thomas Maschler",
    license="MIT",
    install_requires=["parallelpipe", "psycopg2-binary==2.8.6", "numpy<1.18", "rasterio==1.2.0", "boto3<1.13"],
    # Need numpy<1.18 because more recent versions require a more recent version of Python3.
    # Need psycopg2-binary==2.8.6 and rasterio==1.2.0 because more recent versions than that used in March 2021 don't install.
    scripts=["gfw_tile_prep/prep_tiles.py"],
)

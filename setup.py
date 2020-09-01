from setuptools import setup

setup(
    name="gfw_tile_prep",
    version="0.1.0",
    description="Tool to preprocess GFW tiles",
    packages=["gfw_tile_prep"],
    author="Thomas Maschler",
    license="MIT",
    install_requires=["parallelpipe", "psycopg2-binary", "numpy<1.18", "rasterio", "boto3<1.13"],
    # Need numpy<1.18 because more recent versions require a more recent version of Python3
    scripts=["gfw_tile_prep/prep_tiles.py"],
)

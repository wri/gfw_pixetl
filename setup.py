from setuptools import setup

setup(
    name="gfw_tile_prep",
    version="0.1.0",
    description="Tool to preprocess GFW tiles",
    packages=["gfw_tile_prep"],
    author="Thomas Maschler",
    license="MIT",
    install_requires=[
        "boto3~=1.10.1",
        "click~=7.0",
        "parallelpipe~=0.2.6",
        "psycopg2-binary~=2.8.4",
        "pyproj~=2.4.0",
        "pyyaml~=5.1.2",
        "rasterio[s3]~=1.1.0",
        "shapely~=1.6.4.post2",
    ],
    scripts=["gfw_tile_prep/tile_prep.py"],
)

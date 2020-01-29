from setuptools import setup, find_packages

setup(
    name="gfw_pixetl",
    version="0.3.0",
    description="Tool to preprocess GFW tiles",
    packages=find_packages(exclude=("tests",)),
    author="Thomas Maschler",
    license="MIT",
    install_requires=[
        "boto3~=1.10.1",
        "click~=7.0",
        "geojson~=2.5.0",
        "parallelpipe~=0.2.6",
        "psutil~=5.6.7",
        "psycopg2~=2.8.4",
        "pyproj~=2.4.0",
        "pyyaml~=5.1.2",
        "rasterio[s3]~=1.1.0",
        "retrying~=1.3.3",
        "shapely~=1.6.4.post2",
    ],
    entry_points="""
            [console_scripts]
            pixetl=gfw_pixetl.pixetl:cli
            """,
)

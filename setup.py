from setuptools import setup

setup(
    name="gfw_tile_prep",
    version="0.1.0",
    description="Tool to preprocess GFW tiles",
    packages=["gfw_tile_prep"],
    author="Thomas Maschler",
    license="MIT",
    install_requires=["parallelpipe, psycopg2"],
)

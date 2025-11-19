from setuptools import setup, find_packages

setup(
    name="motor-ingesta",
    version="0.1.0",
    author="Óscar Rico Rodríguez",
    author_email="oscarico@ucm.es",
    description="Ingestion engine for the Spark course",
    long_description="Ingestion engine for the Spark course",
    long_description_content_type="text/markdown",
    url="https://github.com/orr21",
    python_requires=">=3.8",
    packages=find_packages(),
    package_data={"motor_ingesta": ["resources/*.csv"]}
)

from setuptools import setup, find_packages

setup(
    name="finnhub_client",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "finnhub-python",
        "avro-python3",
        "websocket-client",
        "confluent-kafka",
        "requests"
    ],
) 
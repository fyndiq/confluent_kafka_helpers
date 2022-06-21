from setuptools import find_packages, setup

setup(
    name="confluent-kafka-helpers",
    version="0.10.2",
    description="Helpers for Confluent's Kafka Python client",
    url="https://github.com/fyndiq/confluent_kafka_helpers",
    author="Fyndiq AB",
    author_email="support@fyndiq.com",
    license="MIT",
    packages=find_packages(),
    setup_requires=['wheel'],
    install_requires=[
        'structlog>=17.2.0',
        'confluent-kafka>=1.0.0,<=1.9.0',
        'fastavro>=0.18.0,<=1.5.1',
        'avro-python3>=1.8.2,<=1.10.2',
    ],
    extras_require={'opentracing': 'opentracing>=2.4.0'},
    zip_safe=False,
)

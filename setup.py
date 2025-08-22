from setuptools import find_packages, setup

setup(
    name="confluent-kafka-helpers",
    version="1.1.0",
    description="Helpers for Confluent's Kafka Python client",
    url="https://github.com/fyndiq/confluent_kafka_helpers",
    author="Fyndiq AB",
    author_email="support@fyndiq.com",
    license="MIT",
    packages=find_packages(),
    setup_requires=["wheel"],
    install_requires=[
        "structlog>=17.2.0",
        "confluent-kafka[avro]==2.*",
        "opentelemetry-api==1.27.0",
        "opentelemetry-semantic-conventions==0.48b0",  # 1.27.0
    ],
    zip_safe=False,
)

from setuptools import find_packages, setup


def get_requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


setup(
    name="confluent-kafka-helpers",
    version="0.7.3",
    description="Helpers for Confluent's Kafka Python client",
    url="https://github.com/fyndiq/confluent_kafka_helpers",
    author="Fyndiq AB",
    author_email="support@fyndiq.com",
    license="MIT",
    packages=find_packages(),
    install_requires=get_requirements(),
    zip_safe=False,
    entry_points={
        "console_scripts": [
            "schema-registry=confluent_kafka_helpers.bin.schema_registry.__main__:main"
        ]
    },
)

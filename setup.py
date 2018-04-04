from setuptools import find_packages, setup

setup(name="confluent_kafka_helpers",
      version="0.5.2",
      description="Helpers for Confluent's Kafka Python client",
      url="https://github.com/fyndiq/confluent_kafka_helpers",
      author="Fyndiq AB",
      author_email="support@fyndiq.com",
      license="MIT",
      packages=find_packages(),
      install_requires=[
          'structlog>=17.2.0',
          'git+https://github.com/confluentinc/confluent-kafka-python@v0.11.4#egg=confluent-kafka[avro]',
          'fastavro[ujson]==0.16.7'
      ],
      zip_safe=False)

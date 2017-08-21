from setuptools import setup

setup(name="confluent_kafka_helpers",
      version="0.1",
      description="Helpers for Confluent's Kafka Python client",
      url="https://github.com/fyndiq/confluent_kafka_helpers",
      author="Fyndiq AB",
      author_email="support@fyndiq.com",
      license="MIT",
      packages=["confluent_kafka_helpers"],
      install_requires=[
          'confluent-kafka==0.11.0',
      ],
      zip_safe=False)

from setuptools import setup, find_packages

setup(name="confluent_kafka_helpers",
      version="0.1",
      description="Helpers for Confluent's Kafka Python client",
      url="https://github.com/fyndiq/confluent_kafka_helpers",
      author="Fyndiq AB",
      author_email="support@fyndiq.com",
      license="MIT",
      packages=find_packages(),
      install_requires=[
          'structlog>=17.2.0',
          'requests>=2.18.3',
          'confluent-kafka[avro]>=0.0.0'
      ],
      dependency_links=[
          'git+https://github.com/confluentinc/confluent-kafka-python#egg=confluent-kafka[avro]-999.0.0'
      ],
      zip_safe=False)

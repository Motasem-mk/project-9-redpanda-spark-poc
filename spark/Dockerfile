FROM apache/spark:3.5.0

USER root
WORKDIR /app

COPY spark_consumer.py .
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "spark_consumer.py"]

FROM flink:1.18.1-scala_2.12

USER root

RUN apt-get update -qq && \
    apt-get install -y -qq python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python && \
    python3 -m pip install --no-cache-dir apache-flink==1.18.1 psycopg2-binary && \
    python -c "import pyflink; import psycopg2; print('PyFlink and psycopg2 installed successfully')"

RUN curl -fL -o /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar && \
    chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar && \
    chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar && \
    test -f /opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar || (echo "Kafka connector download failed" && exit 1)

COPY jars/*.jar /opt/flink/lib/

USER flink

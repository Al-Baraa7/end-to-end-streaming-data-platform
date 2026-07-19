FROM eclipse-temurin:11-jre-jammy AS spark-base
ARG SPARK_VERSION=3.5.7
RUN apt-get update && apt-get install -y curl procps python3 python3-pip && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | tar -xz -C /opt/
RUN mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark


ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_SUBMIT_OPTS="-Dio.netty.tryReflectionSetAccessible=true"

RUN pip3 install --no-cache-dir \
    faker \
    psycopg2-binary \
    pymongo \
    kafka-python-ng \
    python-dotenv && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar -o /opt/spark/jars/postgresql-42.7.1.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar -o /opt/spark/jars/mongo-spark-connector_2.12-10.7.0.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/mongodb/mongodb-driver-sync/5.1.1/mongodb-driver-sync-5.1.1.jar -o /opt/spark/jars/mongodb-driver-sync-5.1.1.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/mongodb/mongodb-driver-core/5.1.1/mongodb-driver-core-5.1.1.jar -o /opt/spark/jars/mongodb-driver-core-5.1.1.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/mongodb/bson/5.1.1/bson-5.1.1.jar -o /opt/spark/jars/bson-5.1.1.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.7/spark-token-provider-kafka-0-10_2.12-3.5.7.jar -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar -o /opt/spark/jars/kafka-clients-3.4.1.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -o /opt/spark/jars/commons-pool2-2.11.1.jar && \
    curl -fsSL https://repo.maven.apache.org/maven2/org/duckdb/duckdb_jdbc/1.1.3/duckdb_jdbc-1.1.3.jar -o /opt/spark/jars/duckdb_jdbc-1.1.3.jar

FROM spark-base AS jupyter
RUN pip3 install --no-cache-dir \
        jupyterlab \
        pyspark==3.5.7 && \
    useradd -m -u 1000 jovyan && \
    mkdir -p /home/jovyan/work && \
    chown -R jovyan:jovyan /home/jovyan

USER jovyan
WORKDIR /home/jovyan/work

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''"]

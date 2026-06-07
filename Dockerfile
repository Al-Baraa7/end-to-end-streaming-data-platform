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

FROM spark-base AS jupyter
RUN pip3 install jupyterlab pyspark==3.5.7

RUN useradd -m -u 1000 jovyan
RUN mkdir -p /home/jovyan/work && chown -R jovyan:jovyan /home/jovyan
USER jovyan
WORKDIR /home/jovyan/work

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''"]
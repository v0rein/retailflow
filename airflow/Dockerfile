FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Pre-create the Spark/Ivy jar cache as the airflow user. The compose file
# mounts a named volume here, and Docker copies this ownership into the volume
# on first use. Without it the volume is root-owned and Spark can't write jars.
RUN mkdir -p /home/airflow/.ivy2

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

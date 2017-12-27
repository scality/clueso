FROM scality/clueso-spark-base:latest

HEALTHCHECK CMD curl -f http://localhost:8080/ || exit 1

WORKDIR /

# Install S3 client
RUN apt-get update && \
    apt-get install -y cron python-pip && \
    pip install boto3

ADD crontab /etc/cron.d/compactor-cron
RUN (crontab -l; cat /etc/cron.d/compactor-cron | crontab; crontab -l)

# Prevents supervisor from starting ingestion prematurely
RUN service supervisor stop

COPY supervisord.conf /etc/supervisor/supervisord.conf
COPY master.sh /
COPY runTasks.py /
COPY startup.sh /
COPY getPensieveCreds-linux /

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/startup.sh"]

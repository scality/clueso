FROM scality/spark-base:hadoop2.8-2.2

HEALTHCHECK CMD curl -f http://localhost:8080/ || exit 1

WORKDIR /

# Install S3 client
RUN apt-get update && \
    apt-get install -y python-pip && \
    pip install boto3

COPY master.sh /
COPY runTasks.py /

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
# check what these logs are -- make sure don't grow too much. if do, need to send to regular logs
# so can set max through stack and rotation
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/master.sh"]

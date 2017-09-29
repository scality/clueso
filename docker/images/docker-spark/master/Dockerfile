#FROM bde2020/spark-base:2.1.0-hadoop2.8-hive-java8
FROM scality/spark-base:hadoop2.8

HEALTHCHECK CMD curl -f http://localhost:8080/ || exit 1

WORKDIR /

COPY master.sh /
RUN chmod +x /master.sh

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/master.sh"]
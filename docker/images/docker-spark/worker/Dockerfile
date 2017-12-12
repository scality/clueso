FROM scality/clueso-spark-base:latest

HEALTHCHECK CMD curl -f http://localhost:8081/ || exit 1

COPY worker.sh /
RUN chmod +x /worker.sh

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

EXPOSE 8081 19998 19999

CMD ["/bin/bash", "/worker.sh"]

FROM scality/spark-base:hadoop2.8-2.2

HEALTHCHECK CMD curl -f http://localhost:8081/ || exit 1

COPY worker.sh /
RUN chmod +x /worker.sh

ENV SPARK_WORKER_WEBUI_PORT 8081
# check on what these logs are.  don't want to accumulate in the image.
# send to standard output?
# if just one line don't care.  need to make sure doesn't grow.
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

EXPOSE 8081 19998 19999

CMD ["/bin/bash", "/worker.sh"]

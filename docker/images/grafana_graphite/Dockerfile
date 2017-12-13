FROM kamon/grafana_graphite

COPY ./clueso-dashboard.json /src/dashboards/

EXPOSE 80 81 2003 8125 8126

CMD ["/usr/bin/supervisord", "--nodaemon", "--configuration", "/etc/supervisor/conf.d/supervisord.conf"]

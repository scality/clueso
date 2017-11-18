FROM kamon/grafana_graphite

ADD ./conf/grafana/dashboards/* /src/dashboards/

EXPOSE 80 81 2003 8125 8126

CMD     ["/usr/bin/supervisord"]

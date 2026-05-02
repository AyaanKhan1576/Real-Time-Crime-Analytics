FROM storm:2.6.0

USER root
RUN mkdir -p /var/lib/storm && chown -R storm:storm /var/lib/storm

COPY java-topology/storm.yaml /conf/storm.yaml

ENV STORM_CONF_DIR=/conf

USER storm

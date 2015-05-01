FROM dockerfile/java:oracle-java8
RUN curl https://s3.amazonaws.com/influxdb/influxdb_latest_amd64.deb -o /tmp/influxdb_latest_amd64.deb
RUN dpkg -i /tmp/influxdb_latest_amd64.deb
COPY setup_influxdb.sh /data/setup_influxdb.sh
COPY target/bounce-1.0-SNAPSHOT-jar-with-dependencies.jar /data/bounce.jar
COPY src/test/resources/bounce.properties /data/
COPY docker_entrypoint.sh /data/
RUN mkdir -p /tmp/blobstore
EXPOSE 8080 9000 9001 8086
CMD []
ENTRYPOINT ["/bin/bash", "./docker_entrypoint.sh"]

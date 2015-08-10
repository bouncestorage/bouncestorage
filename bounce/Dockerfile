FROM java:openjdk-8
RUN curl https://s3.amazonaws.com/influxdb/influxdb_0.8.8_amd64.deb -o /tmp/influxdb.deb
RUN dpkg -i /tmp/influxdb.deb
WORKDIR /data
COPY target/bounce-1.0-SNAPSHOT-jar-with-dependencies.jar bounce.jar
COPY setup_influxdb.sh docker_entrypoint.sh docker_dev_entrypoint.sh ./
COPY src/test/resources/bounce.properties ./
RUN mkdir -p /tmp/blobstore
EXPOSE 80 443 8080 9000 9001
CMD []
ENTRYPOINT ["/data/docker_entrypoint.sh"]

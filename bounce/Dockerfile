FROM java:openjdk-8
RUN curl https://s3.amazonaws.com/influxdb/influxdb_0.8.8_amd64.deb -o /tmp/influxdb.deb
RUN dpkg -i /tmp/influxdb.deb
RUN apt-get update && apt-get -y install less vim
WORKDIR /data
RUN chmod 0600 /etc/java-8-openjdk/management/jmxremote.password
COPY jstatd.all.policy setup_influxdb.sh docker_entrypoint.sh docker_dev_entrypoint.sh ./
COPY target/bounce-1.0-SNAPSHOT-jar-with-dependencies.jar bounce.jar
COPY src/test/resources/bounce.properties ./
RUN mkdir -p /tmp/blobstore
EXPOSE 80 443 8080 9000 9001
CMD []
ENTRYPOINT ["/data/docker_entrypoint.sh"]

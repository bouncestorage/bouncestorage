FROM dockerfile/java:oracle-java8
RUN apt-get update && apt-get install -y maven
COPY pom.xml /data/
RUN mvn verify clean --fail-never
COPY src /data/src
RUN mvn package -DskipTest
RUN find /data -name \*.java -delete
EXPOSE 8080
CMD []
ENTRYPOINT [ "./target/bounce", "--properties", "src/test/resources/bounce.properties" ]

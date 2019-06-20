# First stage to build the application
FROM maven:slim AS build-env
ADD ./pom.xml pom.xml
ADD ./src src/
RUN mvn clean package

# build runtime image
FROM openjdk:11-slim

ENV bootstrapServers="104.41.134.148:31090,40.121.16.99:31090,40.76.11.25:31090" \
topic="badgesource" \
groupId="foo" \
schemaRegistryUrl="http://52.224.220.28:8081" 

# Add the application's jar to the container
COPY --from=build-env target/kafka-consumer-latest-jar-with-dependencies.jar kafka-consumer.jar
#COPY target/kafka-consumer-latest-jar-with-dependencies.jar kafka-consumer.jar

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/kafka-consumer.jar"]
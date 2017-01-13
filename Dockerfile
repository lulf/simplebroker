FROM openjdk:8-jre-alpine

RUN apk add --no-cache bash
ADD build/distributions/simplebroker.tar /

ENV JAVA_OPTS "-Xmx16m -Xms16m"

CMD ["/simplebroker/bin/simplebroker"]

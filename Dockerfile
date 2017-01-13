FROM openjdk:8-jre-alpine

RUN apk add --no-cache bash
ADD build/distributions/simplebroker.tar /

CMD ["/simplebroker/bin/simplebroker"]

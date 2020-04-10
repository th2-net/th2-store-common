FROM openjdk:8-alpine
ENV CRADLE_INSTANCE_NAME=instance1 \
    CASSANDRA_DATA_CENTER=kos \
    CASSANDRA_HOST=cassandra \
    CASSANDRA_PORT=9042 \
    CASSANDRA_KEYSPACE=demo \
    CASSANDRA_USERNAME=guest \
    CASSANDRA_PASSWORD=guest \
    GRPC_PORT=9095
WORKDIR /home
COPY ./ .
ENTRYPOINT ["/home/event-store/bin/event-store", "/home/event-store/etc/config.yml"]

FROM confluentinc/cp-kafka-connect:7.5.0

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
RUN confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:latest
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.0.3

COPY patch/MinioS3Storage.java /tmp/MinioS3Storage.java

RUN mkdir -p /tmp/patch-classes && \
    CP=$(ls /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/*.jar \
            /usr/share/java/kafka/*.jar \
            /usr/share/java/confluent-common/*.jar \
            2>/dev/null | tr '\n' ':') && \
    javac -cp "$CP" /tmp/MinioS3Storage.java -d /tmp/patch-classes && \
    S3JAR=$(ls /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/kafka-connect-s3-*.jar) && \
    jar uf "$S3JAR" -C /tmp/patch-classes .
FROM apache/hive:4.0.0

# Download and install PostgreSQL JDBC driver and AWS SDK
USER root
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O /opt/hive/lib/postgresql-jdbc.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.262.jar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER hive

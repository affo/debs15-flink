FROM maven:3.3.3-jdk-8

RUN mkdir /opt/flink
RUN git clone https://github.com/apache/flink.git /opt/flink
WORKDIR /opt/flink
RUN mvn clean package -DskipTests # this will take up to 5 minutes
ENV FLINK_HOME /opt/flink/build-target
ENV PATH $FLINK_HOME/bin:$PATH

ENTRYPOINT [ "flink" ]

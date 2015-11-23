FROM affo/flink:0.10.0

# RUN apt-get update && apt-get install -qy netcat-openbsd
ADD build/libs/debs15-flink.jar $FLINK_HOME/lib/
ADD build/libs/debs15-flink.jar ./
ADD flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml
ADD run.sh ./
ADD src/main/resources/data.sample.csv /input_data.csv

ENTRYPOINT [ "./run.sh" ]

TEST_IMG=debs15-flink-test
MAIN_CLASS=it.affo.phd.debs15.flink.Main
OPTS=

all: run

run: clean
	# building with gradle wrapper
	./gradlew build
	# running with docker
	sleep 2
	docker build -t $(TEST_IMG) .
	docker run -it -d -p 48081:8081 --name debs15-running $(TEST_IMG) -c$(MAIN_CLASS) $(OPTS)
	# now run `make attach` to see output

attach:
	docker attach debs15-running
	# run `make end` to copy output file

end:
	docker cp debs15-running:/output.data .

clean:
	docker rm -f debs15-running; true

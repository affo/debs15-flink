TEST_IMG=debs15-flink-test
MAIN_CLASS=it.affo.phd.debs15.flink.Main
OPTS=

build_run: clean
	# building with gradle wrapper
	./gradlew build
	# running with docker
	sleep 2
	docker build -t $(TEST_IMG) .
	docker run -it -p 48081:8081 --name debs15-running $(TEST_IMG) -c$(MAIN_CLASS) $(OPTS)

clean:
	docker rm -f debs15-running; true
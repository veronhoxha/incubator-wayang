.PHONY: build_and_run_wayang

build_and_run_wayang:
	./mvnw clean install -DskipTests -Drat.skip=true
	./mvnw clean package -pl :wayang-assembly -Pdistribution
	cd wayang-assembly/target && tar -xvf apache-wayang-assembly-0.7.1-incubating-dist.tar.gz && \
	cd wayang-0.7.1 && \
	./bin/wayang-submit org.apache.wayang.apps.wordcount.ParquetWordCountJob java \
	file://$(PWD)/data/test-00000-of-00001.csv file://$(PWD)/data/test-00000-of-00001.parquet && \
	./bin/wayang-submit org.apache.wayang.apps.wordcount.ParquetWordCountJob java \
	file://$(PWD)/data/train-00000-of-00001.csv file://$(PWD)/data/train-00000-of-00001.parquet

all: build_and_run_wayang

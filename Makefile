# Makefile for mapreduce-on-stackoverflow-dataset
#
# @author arensonzz
# project_name:=mapreduce-on-stackoverflow-dataset

# Source default environment variables from .env file
include .env

.DELETE_ON_ERROR:

# Default target
all: up wordcount

up:
	# Start Hadoop nodes
	docker compose up -d
	# Create volume for Maven local repository
	docker volume create --name maven-repo

wordcount:
	# Run wordcount example using HDFS and MapReduce
	./hdfs dfs -rm -r -f /test_input
	./hdfs dfs -mkdir -p /test_input
	./hdfs dfs -put /app/data/test.txt /test_input
	./hdfs dfs -rm -r -f /test_output
	./hadoop jar /app/jars/WordCount.jar WordCount /test_input /test_output
	./hdfs dfs -get /test_output /app/res
	./hdfs dfs -ls /test_output
	./hdfs dfs -cat /test_output/part*

move-data:
	# Move input files inside "jobs/data" to the HDFS.
	@./hdfs dfs -mkdir -p /input
	@./hdfs dfs -put /app/data/Questions.csv /input || exit 0
	@./hdfs dfs -put /app/data/Answers.csv /input || exit 0

upvote-stats: copy-jar
	# Run UpvoteStatistics job
	./hadoop jar /app/jars/mapreduce-stackoverflow-1.0.jar Driver

text-stats: copy-jar
	# Run TextStatistics job
	./hdfs dfs -rm -r -f /output/text-stats
	./hadoop jar /app/jars/mapreduce-stackoverflow-1.0.jar TextMain
	./hdfs dfs -get /output /app/res
	./hdfs dfs -ls /output/text-stats
	./hdfs dfs -cat /output/text-stats/part*

mvn: _mvn-package copy-jar _mvn-clean

_mvn-package:
	# Compile the project using "mvn" and package it in a JAR file.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/mymaven -v maven-repo:/root/.m2 -w /usr/src/mymaven maven:3.3-jdk-8 mvn clean package

_mvn-clean:
	# Clear the "mapreduce/target" directory.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/tmp alpine find /usr/src/tmp/target /usr/src/tmp/dependency-reduced-pom.xml -delete || exit 0
	
copy-jar:
	# Copy the JAR file located in "mapreduce/target" to "jobs/jars".
	cp mapreduce/target/mapreduce-stackoverflow-1.0.jar jobs/jars

clean: _mvn-clean
	# Clear the "jobs/res" directory.
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res -mindepth 1 -delete || exit 0
	@touch jobs/res/.gitkeep
	# Stop Hadoop cluster.
	docker compose down --volumes

.PHONY: all build run clean up wordcount move-data upvote-stats text-stats mvn _mvn-package _mvn-clean copy-jar 

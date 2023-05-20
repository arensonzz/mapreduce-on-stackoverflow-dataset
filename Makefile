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
	./hdfs dfs -rm -r -f /input
	./hdfs dfs -mkdir -p /input
	./hdfs dfs -put /app/data/Questions.csv /input
	./hdfs dfs -put /app/data/Answers.csv /input

upvote-stats: copy-jar
	# Run UpvoteStatistics job
	./hdfs dfs -rm -r -f /output/upvote-stats
	./hadoop jar /app/jars/mapreduce-stackoverflow-1.0.jar AnswerQuestionMain
	./hdfs dfs -get /output /app/res
	./hdfs dfs -ls /output/upvote-stats
	./hdfs dfs -cat /output/upvote-stats/part*

text-stats: copy-jar
	# Run TextStatistics job
	./hdfs dfs -rm -r -f /output/text-stats
	./hadoop jar /app/jars/mapreduce-stackoverflow-1.0.jar TextMain
	./hdfs dfs -get /output /app/res
	./hdfs dfs -ls /output/text-stats
	./hdfs dfs -cat /output/text-stats/part*


mvn: _mvn-package copy-jar
	# Package project using mvn and copy the JAR file to jobs/jars.
	docker run -it -v "$(shell pwd)/mapreduce":/usr/src/mymaven -w /usr/src/mymaven maven:3.3-jdk-8 mvn clean 
	

_mvn-package:
	# Compile the project using mvn and package it in a JAR file.
	docker run -it -v "$(shell pwd)/mapreduce":/usr/src/mymaven -w /usr/src/mymaven maven:3.3-jdk-8 mvn clean package
	
copy-jar:
	# Copy the JAR file located in target to jobs/jars.
	cp mapreduce/target/mapreduce-stackoverflow-1.0.jar jobs/jars

clean:
	# Clear the /jobs/res directory
	docker exec namenode find /app/res -mindepth 1 -delete
	@touch jobs/res/.gitkeep
	# Stop Hadoop cluster
	docker compose down --volumes


.PHONY: all build run clean up wordcount mvn-package copy-jar upvote-stats text-stats

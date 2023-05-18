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
	# Starting Hadoop nodes
	docker compose up -d

wordcount:
	# Running wordcount example using HDFS and MapReduce
	./hdfs dfs -rm -r -f /test_input
	./hdfs dfs -mkdir -p /test_input
	./hdfs dfs -put /app/data/test.txt /test_input
	./hdfs dfs -rm -r -f /test_output
	./hadoop jar /app/jars/WordCount.jar WordCount /test_input /test_output
	./hdfs  dfs -ls /test_output
	./hdfs  dfs -cat /test_output/part*

clean:
	# Clean target run
	docker compose down --volumes


.PHONY: all build run clean up

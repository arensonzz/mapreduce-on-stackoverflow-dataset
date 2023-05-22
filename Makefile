# Makefile for mapreduce-on-stackoverflow-dataset
#
# @author arensonzz
# project_name:=mapreduce-on-stackoverflow-dataset

# Source default environment variables from .env file
include .env

.DELETE_ON_ERROR:

# Default target
all: up wordcount

#
# General Targets
#
up:
	# Start Hadoop nodes
	docker compose up -d
	# Create volume for Maven local repository
	docker volume create --name maven-repo

move-data:
	# Move input files inside "jobs/data" to the HDFS.
	@./hdfs dfs -mkdir -p /input
	@./hdfs dfs -mkdir -p /input/small
	@./hdfs dfs -put /app/data/test.txt /input || exit 0
	@./hdfs dfs -put /app/data/Questions.csv /input || exit 0
	@./hdfs dfs -put /app/data/Answers.csv /input || exit 0
	@./hdfs dfs -put /app/data/Tags.csv /input || exit 0

	@./hdfs dfs -put /app/data/QuestionsSmall.csv /input/small || exit 0
	@./hdfs dfs -put /app/data/AnswersSmall.csv /input/small || exit 0
	@./hdfs dfs -put /app/data/TagsSmall.csv /input/small || exit 0

mvn: _mvn-package copy-jar _mvn-clean

copy-jar:
	# Copy the JAR file located in "mapreduce/target" to "jobs/jars".
	cp mapreduce/target/${MAIN_JAR_NAME} jobs/jars

clean: _mvn-clean
	# Clear the "jobs/res" directory.
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res -mindepth 1 -delete || exit 0
	@touch jobs/res/.gitkeep
	# Stop Hadoop cluster.
	docker compose down --volumes

#
# MapReduce Job Targets
#
wordcount-src:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} WordCountSrc /input/small/QuestionsSmall.csv /output/${@}
	# Output files:
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head

# Directory as input to MapReduce
wordcount-all:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} WordCountSrc /input/small /output/${@}
	# Output files:
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head


upvote-stats:
	# Run UpvoteStatistics job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} Driver

text-stats:
	# Run TextStatistics job
	./hdfs dfs -rm -r -f /output/text-stats
	./hadoop jar /app/jars/${MAIN_JAR_NAME} TextMain
	./hdfs dfs -get /output /app/res
	./hdfs dfs -ls /output/text-stats
	./hdfs dfs -cat /output/text-stats/part*

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

#
# Private Targets
#
_mvn-package:
	# Compile the project using "mvn" and package it in a JAR file.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/mymaven -v maven-repo:/root/.m2 -w /usr/src/mymaven maven:3.3-jdk-8 mvn clean package

_mvn-clean:
	# Clear the "mapreduce/target" directory.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/tmp alpine find /usr/src/tmp/target /usr/src/tmp/dependency-reduced-pom.xml -delete || exit 0


.PHONY: all up move-data mvn copy-jar clean wordcount-src upvote-stats text-stats wordcount _mvn-package _mvn-clean

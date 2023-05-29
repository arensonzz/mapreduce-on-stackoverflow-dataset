# Makefile for mapreduce-on-stackoverflow-dataset
#
# @author arensonzz
# project_name:=mapreduce-on-stackoverflow-dataset

# Source default environment variables from .env file
include .env
DATA_FILES:=jobs/data/QuestionsPre.csv jobs/data/AnswersPre.csv


.DELETE_ON_ERROR:

# Default target
all: ready

#
# General Targets
#
ready: clean up move-data mvn

up:
	# Start Hadoop nodes
	docker compose up -d
	# Create volume for Maven local repository
	docker volume create --name maven-repo

move-data: ${DATA_FILES}
	# Move input files inside "jobs/data" to the HDFS.
	@./hdfs dfs -mkdir -p /input
	@./hdfs dfs -put /app/data/test.txt /input || exit 0
	@./hdfs dfs -put /app/data/QuestionsPre.csv /input || exit 0
	@./hdfs dfs -put /app/data/AnswersPre.csv /input || exit 0
	@./hdfs dfs -put /app/data/QuestionsSmallPre.csv /input || exit 0
	@./hdfs dfs -put /app/data/AnswersSmallPre.csv /input || exit 0


mvn: _mvn-package copy-jar _mvn-clean

copy-jar:
	# Copy the JAR file located in "mapreduce/target" to "jobs/jars".
	cp mapreduce/target/${MAIN_JAR_NAME} jobs/jars

preprocess:
	@docker build -t "${PROJECT_NAME}-python" python
	# Preprocess CSV files using Python script
	@docker run -it -v "$(shell pwd)/jobs/data":/usr/src/jobs/data -v "$(shell pwd)/python":/usr/src/python ${PROJECT_NAME}-python python3 python/preprocess.py


clean: _mvn-clean
	# Clear the "jobs/res" directory.
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res -mindepth 1 -delete || exit 0
	@touch jobs/res/.gitkeep
	# Stop Hadoop cluster.
	docker compose down --volumes

#
# MapReduce Job Targets
#
users-question-score-sum:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} UsersQuestionScoreSum /input/QuestionsPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head


question-tfidf:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} TFIDFQuestionBody /input/QuestionsPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head


question-statistics:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} QuestionScoreStatistics /input/QuestionsPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head


answer-statistics:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} AnswerScoreStatistics /input/AnswersPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head

wc-answers:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} AnswerWordCount /input/AnswersPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head

wc-questions-body:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} QuestionBodyWordCount /input/QuestionsPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head

wc-questions-title:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} QuestionTitleWordCount /input/QuestionsPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head

yearly-trend-topics:
	# Clear the output directory
	./hdfs dfs -rm -r -f /output/${@}
	@docker run -it -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${@} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} YearlyTrendTopics /input/QuestionsPre.csv /output/${@}
	# Output files:
	./hdfs dfs -get /output/${@} /app/res
	./hdfs dfs -ls /output/${@}
	./hdfs dfs -cat /output/${@}/part* | head


#
# Private Targets
#
_mvn-package:
	# Compile the project using "mvn" and package it in a JAR file.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/mymaven -v maven-repo:/root/.m2 -w /usr/src/mymaven maven:3.3-jdk-8 mvn clean package

_mvn-clean:
	# Clear the "mapreduce/target" directory.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/tmp alpine find /usr/src/tmp/target /usr/src/tmp/dependency-reduced-pom.xml -delete || exit 0

${DATA_FILES}:
	@echo "You do not have the needed input files."
	@echo "1. Move Questions.csv, Answers.csv and Tags.csv into \"jobs/data\" folder."
	@echo "2. Run the preprocessing script. You can use the Docker image to do that or use your own Python binaries."
	@printf "\tDocker\t\t: make preprocess\n"
	@printf "\tOR\n"
	@printf "\tLocal Python\t: python3 python/preprocess.py\n"
	exit 1

.PHONY: all ready up move-data mvn copy-jar preprocess clean users-question-score-sum question-tfidf question-statistics answer-statistics wc-answers wc-questions-body wc-questions-title yearly-trend-topics _mvn-package _mvn-clean

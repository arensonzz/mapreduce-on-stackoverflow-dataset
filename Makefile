# Makefile for mapreduce-on-stackoverflow-dataset
#
# @author arensonzz

# Source default environment variables from .env file
include .env

# Define Variables
OUTPUT_PATH_FOLDER:=$(lastword ,$(subst /, ,${OUTPUT_PATH}))


.DELETE_ON_ERROR:

all:
	@echo "You can start the Hadoop cluster with \`make ready\` and then use \`make run\` to start the GUI."

#
# General Targets
#
ready: clean up mvn


run:
	java -cp jobs/jars/${MAIN_JAR_NAME} UserInterface


up:
	# Start Hadoop nodes
	docker compose up -d
	# Create volume for Maven local repository
	docker volume create --name maven-repo


move-data: ${DATA_FILES}
	# Move input files inside "jobs/data" to the HDFS.
	@./hdfs dfs -mkdir -p /input
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
	# Clear the output and res directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/"${OUTPUT_PATH_FOLDER}" -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} UsersQuestionScoreSum ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res/
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	sort -n -k 2 -r "jobs/res/${OUTPUT_PATH_FOLDER}/part-r-00000" | head


question-tfidf:
	# Clear the output and res directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${OUTPUT_PATH_FOLDER} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} TFIDFQuestionBody ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	./hdfs dfs -cat ${OUTPUT_PATH}/part* | head


question-statistics:
	# Clear the output directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${OUTPUT_PATH_FOLDER} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} QuestionScoreStatistics ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	./hdfs dfs -cat ${OUTPUT_PATH}/part*


answer-statistics:
	# Clear the output and res directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${OUTPUT_PATH_FOLDER} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} AnswerScoreStatistics ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	./hdfs dfs -cat ${OUTPUT_PATH}/part*


wc-answers:
	# Clear the output and res directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${OUTPUT_PATH_FOLDER} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} AnswerWordCount ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	sort -n -k 2 -r "jobs/res/${OUTPUT_PATH_FOLDER}/part-r-00000" | head


wc-questions-body:
	# Clear the output and res directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${OUTPUT_PATH_FOLDER} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} QuestionBodyWordCount ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	sort -n -k 2 -r "jobs/res/${OUTPUT_PATH_FOLDER}/part-r-00000" | head


wc-questions-title:
	# Clear the output directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${OUTPUT_PATH_FOLDER} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} QuestionTitleWordCount ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	sort -n -k 2 -r "jobs/res/${OUTPUT_PATH_FOLDER}/part-r-00000" | head


yearly-trend-topics:
	# Clear the output directories
	./hdfs dfs -rm -r -f ${OUTPUT_PATH}
	@docker run -v "$(shell pwd)/jobs":/usr/src/tmp alpine find /usr/src/tmp/res/${OUTPUT_PATH_FOLDER} -delete || exit 0
	# Run MapReduce job
	./hadoop jar /app/jars/${MAIN_JAR_NAME} YearlyTrendTopics ${INPUT_PATH} ${OUTPUT_PATH}
	# Output files:
	# 	copy to local filesystem
	./hdfs dfs -get ${OUTPUT_PATH} /app/res
	# 	preview files
	./hdfs dfs -ls ${OUTPUT_PATH}
	./hdfs dfs -cat ${OUTPUT_PATH}/part* | head


#
# Private Targets
#
_mvn-package:
	# Compile the project using "mvn" and package it in a JAR file.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/mymaven -v maven-repo:/root/.m2 -w /usr/src/mymaven maven:3.3-jdk-8 mvn clean package

_mvn-clean:
	# Clear the "mapreduce/target" directory.
	@docker run -it -v "$(shell pwd)/mapreduce":/usr/src/tmp alpine find /usr/src/tmp/target /usr/src/tmp/dependency-reduced-pom.xml -delete || exit 0


.PHONY: all ready run up move-data mvn copy-jar preprocess clean users-question-score-sum question-tfidf question-statistics answer-statistics wc-answers wc-questions-body wc-questions-title yearly-trend-topics _mvn-package _mvn-clean

# MapReduce on Stackoverflow Dataset

Summarizing large question-answer collection of StackOverflow website using text pre-processing
and different descriptive statistics methods based on the MapReduce Framework.


<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
## Contents

- [About](#about)
- [Requirements](#requirements)
- [Run the Application](#run-the-application)
- [Monitor Hadoop Cluster by WebUI](#monitor-hadoop-cluster-by-webui)
- [Technologies](#technologies)
- [License](#license)
- [Credits](#credits)
- [Contributors](#contributors)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## About

This is a MapReduce application written in Java which runs on Apache Hadoop cluster.

<img src="docs/images/finished_mapreduce_job_gui.png"
     alt="GUI of the application and output of finished MapReduce job."
     style="margin: 10px 0; max-width: 100%; border: 1px black solid;" />

## Requirements

Docker Desktop is required (and it is the easiest way to get Docker work on your laptop).

You can download [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/) (both ) or 
[Docker Desktop for Linux](https://docs.docker.com/desktop/install/linux-install/).

## Run the Application

1. First you must start the Hadoop Cluster, and build Java program with Maven. 
    
    To do this you can run: `make ready`

    This will pull the images from Docker Hub and then start needed nodes. 
    This process might take few minutes if you are running it for the first time.
    After the pull is complete, it will wait until all nodes are ready.

1. After the cluster is up, you should start the GUI. 

    To do that simply run: `make run`

1. Before you run the MapReduce jobs you have to insert input data into HDFS. 

    Small samples of input files are located in `jobs/data`. To insert them, use the file selector from GUI and give 
    any destination path for where to upload files. 
    Or you can run `make move-data` to upload them into the default `/input/` path.

    Full files can be downloaded from [StackSample: 10% of Stack Overflow Q&A](https://www.kaggle.com/datasets/stackoverflow/stacksample).
    Before you insert full files, extract them into `jobs/data` and run: `make preprocess` to get them ready for 
    MapReduce jobs. Preprocessed files can be found as `jobs/data/QuestionsPre.csv` and `jobs/data/AnswersPre.csv`.
    
1. To stop the nodes and delete output files, run `make clean`.

## Monitor Hadoop Cluster by WebUI

* Namenode: http://localhost:9870
* Datanode: http://localhost:9864
* Resourcemanager: http://localhost:8088
* Nodemanager: http://localhost:8042
* Historyserver: http://localhost:8188

> Note If you are redirected to a URL like http://119e8b128bd5:8042/ or http://resourcemanager:8088/, change the host name to localhost (i.e. http://localhost:8042/) and it will work. This is because Docker containers use their own IPs which are mapped to different names.

## Technologies

* [Apache Hadoop v3.3.1](https://hadoop.apache.org/)
* [Docker Engine v23.0](https://docs.docker.com/engine/reference/run/)
* [Docker Compose v2.17](https://docs.docker.com/compose/reference/)
* [Java v1.8.0_322](https://www.oracle.com/java/technologies/downloads/)

## License

MapReduce on Stackoverflow Dataset is free software published under the MIT license. See [LICENSE](LICENSE) for details.

## Credits

Docker Hadoop Cluster setup files are taken from [@wxw-matt](https://github.com/wxw-matt/docker-hadoop).

## Contributors

* [Erkan Vatan](https://github.com/arensonzz)
* [Toygar Tanyel](https://github.com/toygarr)

#!/bin/bash

cd /home/pi/distpi62/
export CLASSPATH=$CLASSPATH:/home/pi/distpi62/jsch-0.1.52.jar
javac WorkerInter.java
javac WorkerImpl.java
javac NodeData.java
java WorkerImpl $1

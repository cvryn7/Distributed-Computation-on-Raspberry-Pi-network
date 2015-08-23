#!/bin/bash

export CLASSPATH=$CLASSPATH:/home/pi/distpi62/jsch-0.1.52.jar
javac MasterInter.java
javac MasterImpl.java
javac NodeData.java
java MasterImpl $1

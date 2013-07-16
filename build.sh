#!/bin/bash

BIN=bin/crispy
SRC=src/crispy
HADOOP_LIB=lib/hadoop-core-0.20.205.0.jar
APACHE_COMMONS_LOG_LIB=lib/commons-logging-api-1.0.4.jar
APACHE_COMMONS_CLI_LIB=lib/commons-cli-1.2.jar
AWS_LIB=lib/aws-java-sdk-1.4.7.jar
TARGET=$1

#
# Step 1
# Building the java source into java classes
#
javac -cp $HADOOP_LIB:$APACHE_COMMONS_LOG_LIB:$APACHE_COMMONS_CLI_LIB \
    $SRC/*.java \
    $SRC/io/*.java \
    $SRC/gendist/*.java \
    $SRC/kmerdist/*.java \
    $SRC/hcluster/*.java \
    $SRC/partition/*.java \
    $SRC/test/*.java

# Remove all class files in the directories
rm -f $BIN/*.class
rm -f $BIN/io/*.class
rm -f $BIN/gendist/*.class
rm -f $BIN/kmerdist/*.class
rm -f $BIN/hcluster/*.class
rm -f $BIN/partition/*.class
rm -f $BIN/test/*.class

# Move all new class files into the Bin directories
mv $SRC/*.class -t $BIN
mv $SRC/io/*.class -t $BIN/io
mv $SRC/gendist/*.class -t $BIN/gendist
mv $SRC/kmerdist/*.class -t $BIN/kmerdist
mv $SRC/hcluster/*.class -t $BIN/hcluster
mv $SRC/partition/*.class -t $BIN/partition
mv $SRC/test/*.class -t $BIN/test

#
# Step 2
# Packaging the java classes with jar
#
cd bin
jar cvf $1 crispy
mv $1 -t ..
cd ..

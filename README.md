# Documentation for crispy-hadoop-0.20

## Description
This is an implementation of the CRiSPy method using an Apache Hadoop framework.
Briefly, the method performs a pairwise distance matrix calculation of all reads using a k-mer measure, followed by a hamming distance measure. Subsequently, the reads are clustered using a fast single-likage hierarchical clustering algorithm. Because calculating the pairwise distance matrix is compute expensive and embarassingly parallel, the objective of this dissertation is to use Apache Hadoop to distribute the data across compute nodes, before piping to the single-linkage hierarchical clustering algorithm to cluster.

## Prerequisites
  * Java Open JRE 6 or Java Oracle JRE 6 and above
  * Apache Hadoop version 0.20.205
  * Apache Whirr version 0.8.2 (For deploying Amazon AWS EC2 instances)


## Usage
```
1. Running a job on the local machine

hadoop jar crispy-hadoop-0.20.jar crispy.CrispyTK
  --input <input.fastq>
  --hdfsOutputDir <output directory for hdfs>
  --localOutputDir <local output directory>

2. Running a job on a AWS cluster

Setting up the Environment
export PATH=${PATH}:<path-to-whirr>
export HADOOP_CONF_DIR=~/.whirr/<cluster>
export JAVA_HOME=<path-to-java>

Launch Cluster
whirr launch-cluster --config <properties>
~/.whirr/<cluster>/hadoop-proxy.sh

Run the Job
hadoop jar crispy-hadoop-0.20.jar crispy.CrispyTK
  --input <input.fastq>
  --hdfsOutputDir <output directory for hdfs>
  --localOutputDir <local output directory>
  --blockSize <blockSize>
```

### Description of Parameters

| Options | Type | Default | Summary Description |
|---------|------|---------|---------------------|
| --input | String | Required | The input fastq file (Only fastq file - 4 lines per record) is accepted |
| --hdfsOutputDir | String | Required | The hdfs output directory location |
| --localOutputDir | String | Required | The local output directory location |
| --blockSize | Integer | 5000 | Read-block size. The number of reads that will be partitioned in one block. Total number of reads in a block = 2*n |
| --kmer | Integer | 6 | Length of the contiguous substring used for kmer comparison |
| --kmerDistThreshold | Double | 0.5 | The threshold distance used to filter away non-similar read pairs. Distances smallers than the threshold are kept and has its genetic distance calculated |
| --hclusterDistThreshold | Double | 0.03 | The distance cutoff threshold |
| --numMapTasks | Integer | 1 | If you have k number of compute nodes, use at least k number of map tasks |
| --bandedK | Double | 0.25 | The width of the DP matrix that will be computed |
| --match | Integer | 5 | The score for a nucleotide match |
| --mismatch | Integer | -4 | The score for a nucelotide mismatch |
| --gapOpen | Integer | -10 | The score for a gap opening | 
| --gapExtension | Integer | -5 | The score for a gap extension |
| --minClusterSize | Integer | 4 | The minimum number of reads present in the OTU before it can be considered as a cluster. |
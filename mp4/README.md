# CS425_MP4

> Reference:
> https://data-flair.training/blogs/install-apache-spark-multi-node-cluster/
> https://spark.apache.org/docs/2.4.0/streaming-programming-guide.html
> https://www.tutorialspoint.com/pyspark/pyspark_sparkcontext.htm

# Spark work in a cluster
## Setup
### Environment
1. On master node
  *  Download http://spark.apache.org/downloads.html
  *  Edit conf/spark-env.sh

  ```
  export JAVA_HOME=<path-of-Java-installation> (eg: /usr/lib/jvm/java-7-oracle/)
  export SPARK_WORKER_CORES=8
  ```
  *  Edit conf/slaves

  ```
  slave01-IP
  slave02-IP
  ...
  ```
2. On worker node
  * Download http://spark.apache.org/downloads.html

### Start services
  On master run
  ```
  sbin/start-all.sh
  ```

## Submit jobs
```
bin/spark-submit --master spark://fa18-cs425-g55-01.cs.illinois.edu:7077 test_job.py
```

## Spark UI
### Master UI
http://MASTER-IP:8080/
### Application UI
http://MASTER-IP:4040/

# Crane
  * Job can be submitted only on Master (VM # 1) or Client (VM # 10)
  * The jobs are implemented as RPCs.
## Available functions
The following functions are available for execution.
  * setSpout(desc, readin_path)
  * setBolt(desc, category, func, after)
  * print_topology()
  * reset()
To call these functions, use `crane <function>` command.
## Submit
To submit, use `crane submit` command.
## Example 1: Word Count
```
crane setSpout("spout","/home/xjin12/cs425_mp4/test/small.txt")
crane setBolt("split","transform","lambda x: x[0].split()", "spout")
crane setBolt("pair","transform","lambda x: (x,1)", "split")
crane setBolt("count","reduce","lambda x,y: x+y", "pair")
crane submit
```
## Example 2: URL Count
```
crane setSpout("spout","/home/xjin12/cs425_mp4/test/small.txt")
crane setBolt("split","transform","lambda x: x[0].split()", "spout")
crane setBolt("filter","filter","lambda x: x[0]=='h'", "split")
crane setBolt("pair","transform","lambda x: (x,1)", "filter")
crane setBolt("count","reduce","lambda x,y: x+y", "pair")
crane submit
```
## Example 3: Distance-2 Nodes
```
crane setSpout("spout","/home/xjin12/cs425_mp4/test/network_small.txt")
crane setBolt("tuple","transform","lambda x: tuple(map(int,x[0].strip().split()))", "spout")
crane setBolt("join","join","lambda x,y: x[1] == y[0]", "tuple")
crane setBolt("simplify","transform","lambda x: (x[0][0],[x[1][1]])", "join")
crane setBolt("reduce","reduce","lambda x,y: x+y", "simplify")
crane submit
```

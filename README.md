```
$ hadoop jar hadoop.jar ConvertToSequenceFile NBCorpus/Country/ trainSet testSet
18/11/28 21:30:25 INFO zlib.ZlibFactory: Successfully loaded & initialized native-zlib library
18/11/28 21:30:25 INFO compress.CodecPool: Got brand-new compressor [.deflate]
18/11/28 21:30:25 INFO compress.CodecPool: Got brand-new compressor [.deflate]
```
```
$ ll
总用量 50304
drwxr-xr-x 2 memory7734 memory7734     4096 9月  10 11:58 bin
……
drwxrwxr-x 4 memory7734 memory7734     4096 11月 14 17:05 NBCorpus
……
-rw-r--r-- 1 memory7734 memory7734   613399 11月 28 21:30 testSet
-rw-r--r-- 1 memory7734 memory7734  1750520 11月 28 21:30 trainSet
```
```
$ hadoop jar hadoop.jar StatisticsClassAmount trainSet trainSetClassAmount
18/11/28 21:42:29 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
……
	File Input Format Counters 
		Bytes Read=1764204
	File Output Format Counters 
		Bytes Written=196
```
```
$ hdfs dfs -text trainSetClassAmount/part-r-00000 
USA:zoran	1
USA:zorn	1
USA:zurlo	1
USA:zvereva	1
USA:zweig	1
```
```
$ hadoop jar hadoop.jar StatisticsWordAmount trainSetClassAmount/part-r-00000 trainSetWordAmount

18/11/28 21:45:46 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
18/11/28 21:45:46 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
……
	File Input Format Counters 
		Bytes Read=1347054
	File Output Format Counters 
		Bytes Written=196
```
```
$ hdfs dfs -text trainSetWordAmount/part-r-00000 
AUSTR	41551
FRA	32185
INDIA	37678
JAP	35830
UK	94290
USA	227701
```
```
$ hadoop jar hadoop.jar StatisticsUniqueWordAmount trainSetClassAmount/part-r-00000 trainSetUniqueWordAmount

18/11/28 21:45:46 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
18/11/28 21:45:46 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
……
	File Input Format Counters 
		Bytes Read=1347054
	File Output Format Counters 
		Bytes Written=595950
```
```
$ hdfs dfs -text trainSetUniqueWordAmount/part-r-00000 
zosen	1
zuari	1
zulfikar	1
zurich	1
zurlo	1
zvereva	1
zweig	1
```

```
$ hadoop jar hadoop.jar CalculateProbability trainSetClassAmount/part-r-00000 trainSetWordAmount/part-r-00000 trainSetUniqueWordAmount/part-r-00000 testSet testResult
18/11/29 10:08:36 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
18/11/29 10:08:36 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
……
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=588122
	File Output Format Counters 
		Bytes Written=39039

```
```
$ hdfs dfs -text testResult/part-r-00000
……
488890newsML.txt	FRA
488961newsML.txt	FRA
488975newsML.txt	FRA
488980newsML.txt	USA
488999newsML.txt	FRA
489003newsML.txt	USA
489004newsML.txt	INDIA
489013newsML.txt	USA
489019newsML.txt	AUSTR
489022newsML.txt	USA

```
```
$ hadoop jar hadoop.jar CalculateEvaluation testSet testResult trainSetWordAmount/part-r-00000 evaluation
18/11/29 10:08:36 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
18/11/29 10:08:36 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
……
	Map-Reduce Framework
		Map input records=1378
		Map output records=8268
		Map output bytes=337963
		Map output materialized bytes=354505
		Input split bytes=107
		Combine input records=0
		Combine output records=0
		Reduce input groups=1378
		Reduce shuffle bytes=354505
		Reduce input records=8268
		Reduce output records=1378
		Spilled Records=16536
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=22
		Total committed heap usage (bytes)=953155584
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=618203
	File Output Format Counters 
		Bytes Written=41573
```
```
$ hdfs dfs -text evaluation/part-r-00000 
AUSTR:FN	15
AUSTR:FP	8
AUSTR:TN	1214
AUSTR:TP	59
FRA:FN	15
FRA:FP	13
FRA:TN	1207
FRA:TP	61
INDIA:FN	2
INDIA:FP	3
INDIA:TN	1229
INDIA:TP	62
JAP:FN	17
JAP:FP	6
JAP:TN	1168
JAP:TP	105
UK:FN	19
UK:FP	30
UK:TN	1065
UK:TP	182
USA:FN	19
USA:FP	27
USA:TN	510
USA:TP	740
```
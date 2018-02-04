# indexing-using-pyspark
In this file i'm indexing a vcf format file into elastic search. VCF is a text file format (most likely stored in a compressed manner). It contains meta-information lines, a header line, and then data lines each containing information about a position in the genome.

Here i'm using Elastic-hadoop.jar as a connector between spark and elastic search (http://download.elastic.co/hadoop/elasticsearch-hadoop-6.1.3.zip) and Elasticsearch library in python (pip install elasticsearch).

In this, i'm first reading raw file from HDFS and transforming that file with help og RDD actions in spark and converting using Elasticsearch writeable format. 

After that i'm checked if that index exists in elasticsearch with help of python elasticsearch library, if not then creating new with some low memory cluster dependencies. And then save that RDD into elastic search with help of ES-hadoop connector.

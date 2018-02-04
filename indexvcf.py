from pyspark import SparkContext, SparkConf
from elasticsearch import Elasticsearch

if __name__ == "__main__":
	conf = SparkConf().setAppName("ESTest")
	sc = SparkContext(conf=conf)
	
	ES_COORDINATOR = "localhost" #in my case some ip
	ES_PORT = "9200"
	INDEX_NAME = "test_indexname"
	INDEX_TYPE = "indextype"
	HDFS_FILEPATH = "hdfs://localhost:8020/home/dataSource/filename.vcf"
	
	es = Elasticsearch(hosts = [ES_COORDINATOR])
	es_write_conf = {"es.nodes" : ES_COORDINATOR,"es.port" : ES_PORT,"es.resource" : INDEX_NAME + "/" + INDEX_TYPE}

	def parseVCF(row,columns):
		info = row.split("\t")
		data = {}
		i = 0
		for column in columns:
			data[INDEX_TYPE + "_" + column.strip()] = info[i]
			if(column.strip() == "INFO"):
				for infoCol in info[i].split(";"):
					if "=" in infoCol:
						content = infoCol.split("=")
						data[INDEX_TYPE + "_" + content[0]] = content[1]
					else:
						data[INDEX_TYPE + "_" + infoCol] = "null"
			i+=1
		return ('key', data)
		
	def loadVCFData():
		print("**************************Loading VCF Data***********************************")
		file = sc.textFile(HDFS_FILEPATH)
		
		metadata = file.filter(lambda x: x.startswith("#"))
		schema=""
		for c in metadata.top(1)[0].split("\t"):
			temp = c
			if "#" in c:
				temp = c[1:]
			schema = schema + temp + " "
		
		rows = file.filter(lambda line: not line.startswith("#"))
		content = rows.map(lambda row: parseVCF(row,schema.split()))
		dfd = content.take(2)
		for item in dfd:
			print(item)
		
		content.saveAsNewAPIHadoopFile(
				path='-', 
				outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
				keyClass="org.apache.hadoop.io.NullWritable",
				valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
				conf=es_write_conf
				)
		return "Loaded..."
	
	#Check if index already exist
	if es.indices.exists(INDEX_NAME):
		print("************** Index aready exits *******************")
		result = loadVCFData()
		print(result)
	else:
		request_body = {
			"settings" : {
				"number_of_shards": 1,
				"number_of_replicas": 0,
				"refresh_interval" : -1
				}
			}
		print("*************************** Creating '%s' index **************"%(INDEX_NAME))
		res = es.indices.create(index = INDEX_NAME, body = request_body)
		print(" response: '%s'" % (res))
		loadVCFData()
    

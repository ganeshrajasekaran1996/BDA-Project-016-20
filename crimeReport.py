from pyspark import SparkContext
from pyspark.streaming import StreamingContext,DStream
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
import json
from collections import OrderedDict
import re
import ast
import pyspark
from pyspark.sql.functions import col,isnan, when, count
import pyspark.sql.functions as F
import matplotlib.pyplot as plt	
import seaborn as sns
from pyspark.sql.functions import desc,col
from itertools import chain
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StandardScaler

conf=SparkContext("local[2]","NetworkWordCount")
sc=StreamingContext(conf,1)
#sqc=SQLContext(conf)
spark=SparkSession.builder.appName('CrimeReport').getOrCreate()



def jsonToDf(rdd):

	if not rdd.isEmpty():
		cols=["Date","Category","Description","dayOfweek","District","Resolution","address","X","Y"]
		'''
		cols = StructType([StructField("Date", StringType(), True),\
                                      StructField("Category", StringType(), True),\
                                      StructField("Description", StringType(), True),\
                                      StructField("DayOfWeek", StringType(), True),\
                                      StructField("District", StringType(), True),\
                                      StructField("Resolution", StringType(), True),\
                                      StructField("Address", StringType(), True),\
                                      StructField("X", StringType(), True),\
                                      StructField("Y", StringType(), True)
                                     ])
        '''
		df=spark.read.json(rdd)
		for row in df.rdd.toLocalIterator():
			'''
			r1=".".join(row)
			print(type(r1))
			r2=eval(r1)
			print(type(r2))
			#r2=r1.split("\\n")
			#print(type(r2))
			#result = [k.split(",") for k in r2]
			#print(result)
			'''
			for r in row:

				res=ast.literal_eval(r)
				row1=[]
				for line in res:
					line=re.sub('\\n','',line)
					line=re.sub(r',(?=[^"]*"(?:[^"]*"[^"]*")*[^"]*$)',"",line)
					line=re.sub('"',"",line)
					rowList=line.split(',')
					if not "Dates" in rowList:
						row1.append(rowList)
			#newrdd = conf.parallelize(r2)
			df=spark.createDataFrame(row1,schema=cols)
			df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
			df.dropDuplicates()
			df=df.drop('Descript','Resolution','Address')
			#Convert non numerical to numerical
			data_dict = {'FRAUD':1, 'SUICIDE':2, 'SEX OFFENSES FORCIBLE':3, 'LIQUOR LAWS':4, 
			'SECONDARY CODES':5, 'FAMILY OFFENSES':6, 'MISSING PERSON':7, 'OTHER OFFENSES':8, 
			'DRIVING UNDER THE INFLUENCE':9, 'WARRANTS':10, 'ARSON':11, 'SEX OFFENSES NON FORCIBLE':12,
			'FORGERY/COUNTERFEITING':13, 'GAMBLING':14, 'BRIBERY':15, 'ASSAULT':16, 'DRUNKENNESS':17,
			'EXTORTION':18, 'TREA':19, 'WEAPON LAWS':20, 'LOITERING':21, 'SUSPICIOUS OCC':22, 
			'ROBBERY':23, 'PROSTITUTION':24, 'EMBEZZLEMENT':25, 'BAD CHECKS':26, 'DISORDERLY CONDUCT':27,
			'RUNAWAY':28, 'RECOVERED VEHICLE':29, 'VANDALISM':30,'DRUG/NARCOTIC':31, 
			'PORNOGRAPHY/OBSCENE MAT':32, 'TRESPASS':33,'VEHICLE THEFT':34, 'NON-CRIMINAL':35, 
			'STOLEN PROPERTY':36, 'LARCENY/THEFT':37, 'KIDNAPPING':38,'BURGLARY':39}
			mapping_expr1= create_map([lit(x) for x in chain(*data_dict.items())])
			df=df.withColumn("category", mapping_expr1.getItem(col("Category")))
			pddis={'MISSION':1,'BAYVIEW':2,'CENTRAL':3,'TARAVAL':4, 'TENDERLOIN':5,'INGLESIDE':6, 'PARK':7,'SOUTHERN':8, 'RICHMOND':9,'NORTHERN':10}
			mapping_expr2 = create_map([lit(x) for x in chain(*pddis.items())])
			df=df.withColumn("District", mapping_expr2.getItem(col("PdDistrict")))
			# indexer = StringIndexer(inputCol="Category" , outputCol="category")
			# df= indexer.fit(df).transform(df)
			index= StringIndexer(inputCol="DayOfWeek" , outputCol="DayofWeek")
			df= index.fit(df).transform(df)
			#index= StringIndexer(inputCol="PdDistrict" , outputCol="District")
			# df= index.fit(df).transform(df)
			df = df.withColumn("X", df["X"].cast(DoubleType()))
			df = df.withColumn("Y", df["Y"].cast(DoubleType()))
			df=df.drop('PdDistrict')
			split_col = pyspark.sql.functions.split(df['Date'], ' ')
			df = df.withColumn('Dates', split_col.getItem(0))
			df = df.withColumn('Time', split_col.getItem(1))
			split_col = pyspark.sql.functions.split(df['Date'], '-')
			df = df.withColumn('Year', split_col.getItem(0))
			df = df.withColumn('Month', split_col.getItem(1))
			df = df.withColumn('Day', split_col.getItem(2))
			split_col = pyspark.sql.functions.split(df['Time'], ':')
			df = df.withColumn('Hours', split_col.getItem(0))
			df = df.withColumn('Mins', split_col.getItem(1))
			df = df.withColumn('Secs', split_col.getItem(2))
			df=df.drop('Dates','Time','Date')
			df=df.withColumn("Year",df.Year.cast(IntegerType()))
			df=df.withColumn("Month",df.Month.cast(IntegerType()))
			df=df.withColumn("Day",df.Day.cast(IntegerType()))
			df=df.withColumn("Hours",df.Hours.cast(IntegerType()))
			df=df.withColumn("Mins",df.Mins.cast(IntegerType()))
			df=df.withColumn("Secs",df.Secs.cast(IntegerType()))
			assembler = VectorAssembler(inputCols=['DayofWeek','X','Y','District','Year','Month','Hours','Mins','Secs'],outputCol='features')
			af = assembler.transform(df)
			af=af.select('features')
			#af.show(truncate=False)
			#scaler
			featureScaler = StandardScaler(withMean=False,withStd=True, inputCol='features',outputCol='ss')
			ss = featureScaler.fit(af).transform(af)
			ss = ss.select('ss')
			#ss=ss.append(y_train)
			ss.show(truncate=False)
			#df.show(1000)



lines = sc.socketTextStream("localhost", 6100)
lines.foreachRDD(lambda rdd: jsonToDf(rdd))
#print('##################################################### printing words..')
#print(lines)
#print(type(lines))
sc.start()
sc.awaitTermination()
sc.stop(stopSparkContext=False)





	





```python
#BDS Assignment Submitted by: Sulekha Aloorravi

#Packages to be imported for PySpark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import Row
spark = SparkSession.builder.appName("Python Lab - Sulekha Aloorravi").getOrCreate()
sc = spark.sparkContext
sqlContext = HiveContext(sc)

#Other Python Packages
import datetime #To manipulate Time Stamp
import re #To use regular expressions for Data clean up

```


```python
#Prerequisites before load data from HDFS to RDD
#1. Below variable is used to convert string values of Months into Numeric
monthToNum = {
        'Jan' : 1,
        'Feb' : 2,
        'Mar' : 3,
        'Apr' : 4,
        'May' : 5,
        'Jun' : 6,
        'Jul' : 7,
        'Aug' : 8,
        'Sep' : 9, 
        'Oct' : 10,
        'Nov' : 11,
        'Dec' : 12 }
```


```python
#This function is to store date and time once it is converted into all numerics
def timestamp(x):
    return datetime.datetime(int(x[7:11]), #Year
                             monthToNum[x[3:6]], #month
                             int(x[0:2]), #day
                             int(x[12:14]), #hour
                            )
```


```python
#This function is a regular expression to store and clean up data and make it suitable for Analysis
def comparerecord(input):
    """Add data as key value pairs"""
    comparison = re.search(reg_exp,input)
    if comparison is None:
        return (input, 0)
    bytes = comparison.group(9)
    if bytes == '-':
          size = long(0)
    else:
          size = long(comparison.group(9))            
    return (Row(
            Host = comparison.group(1),
            Hyp1 = comparison.group(2),
            Hyp2 = comparison.group(3),
            TimeStamp = timestamp(comparison.group(4)),
            Get = comparison.group(5) ,
            RequestURL = comparison.group(6),
            Http = comparison.group(7),
            HTTPReplyCode = int(comparison.group(8)),
            BytesTransferred = size
        ),1)
    
    #Data in input_rdd is a raw log input file and it needs to be processed into a format that can be used for analysis
#The above data seems to follow a pattern and can be parsed and converted using regular expressions
reg_exp = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
```


```python
#Read data from HDFS. I have already placed the input file in HDFS directory and the same is loaded here from HDFS.
#This is a function to seggregate records into valid and invalid based on the Regular Expression that was created in 
#previous function

def retrievedata():
    """ Read and Clean up data """
    input_rdd = (sc
             .textFile("NASA_access_log_Aug95.gz")
             .map(comparerecord)
             .cache())

    validrecords = (input_rdd
               .filter(lambda x: x[1] == 1)
                .map(lambda x: x[0])
                .cache())

    discardedrecords = (input_rdd
                    .filter(lambda x: x[1] == 0)
                    .map(lambda x: x[0]))
    discardcount = discardedrecords.count()
    if discardcount > 0:
        print 'No. of records discarded: %d' % discardedrecords.count()
        for line in discardedrecords.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, valid %d lines, discarded %d lines' % (input_rdd.count(), validrecords.count(), discardedrecords.count())
    return input_rdd, validrecords, discardedrecords


input_rdd, validrecords, discardedrecords = retrievedata()
```

    No. of records discarded: 895
    Invalid logline: 198.213.130.253 - - [03/Aug/1995:11:29:02 -0400] "GET /shuttle/missions/sts-34/mission-sts-34.html"><IMG images/ssbuv1.gif SRC="images/small34p.gif/ HTTP/1.0" 404 -
    Invalid logline: ztm-13.dial.xs4all.nl - - [04/Aug/1995:09:34:52 -0400] "GET / /   HTTP/1.0" 200 7034
    Invalid logline: pc32.cis.uoguelph.ca - - [04/Aug/1995:10:57:21 -0400] "GET / /   HTTP/1.0" 200 7034
    Invalid logline: sgate08.st-and.ac.uk - - [04/Aug/1995:17:52:59 -0400] "GET /htbin/wais.pl?Wake Shield HTTP/1.0" 200 6858
    Invalid logline: userp2.snowhill.com - - [05/Aug/1995:14:57:06 -0400] "GET / " HTTP/1.0" 200 7034
    Invalid logline: ppp-nyc-2-64.ios.com - - [05/Aug/1995:20:45:33 -0400] "GET /shuttle/missions/sts-69/images/images.html 40,207 89,234 HTTP/1.0" 200 2443
    Invalid logline: ppp-nyc-2-64.ios.com - - [05/Aug/1995:20:47:52 -0400] "GET /shuttle/countdown/tour.html 40,243 89,262 HTTP/1.0" 200 4347
    Invalid logline: client-71-162.online.apple.com - - [05/Aug/1995:22:53:19 -0400] "GET /msfc/astro home.html HTTP/1.0" 404 -
    Invalid logline: client-71-162.online.apple.com - - [05/Aug/1995:22:53:47 -0400] "GET /msfc/astro home.html HTTP/1.0" 404 -
    Invalid logline: rt99-9.rotterdam.nl.net - - [06/Aug/1995:11:35:40 -0400] "GET / /   HTTP/1.0" 200 7034
    Invalid logline: client-71-69.online.apple.com - - [06/Aug/1995:12:21:18 -0400] "GET /msfc/astro home.html HTTP/1.0" 404 -
    Invalid logline: spark1.ecf.toronto.edu - - [06/Aug/1995:16:13:52 -0400] "GET / history/apollo/apollo-13/apollo-13.html HTTP/1.0" 200 7034
    Invalid logline: bos49.pi.net - - [06/Aug/1995:16:31:40 -0400] "GET /htbin/wais.pl?satellite pictures HTTP/1.0" 200 321
    Invalid logline: 194.151.8.5 - - [07/Aug/1995:08:48:56 -0400] "GET / /   HTTP/1.0" 200 7034
    Invalid logline: www.db.erau.edu - - [07/Aug/1995:11:19:10 -0400] "GET /robots.txt HTML/1.0 headers" 404 -
    Invalid logline: pc532-03.gsfc.nasa.gov - - [07/Aug/1995:12:40:13 -0400] "GET /facilities/mila.html>Merritt Island Spaceflight Tracking and Data Network Station (MILA)</a>" 404 -
    Invalid logline: ara2.acs.muohio.edu - - [07/Aug/1995:20:40:00 -0400] "GET /history/apollo/apollo-13/apollo1-.html    apollo-1 HTTP/1.0" 404 -
    Invalid logline: ara2.acs.muohio.edu - - [07/Aug/1995:20:41:10 -0400] "GET /history/apollo/apollo-13/apollo1-.html    apollo-1 HTTP/1.0" 404 -
    Invalid logline: ara2.acs.muohio.edu - - [07/Aug/1995:20:41:18 -0400] "GET /history/apollo/apollo-13/apollo1-.html    apollo-1 HTTP/1.0" 404 -
    Invalid logline: hyperweb.mit.edu - - [08/Aug/1995:09:01:06 -0400] "GET /shuttle/technology/sts-newsref/http://www.ksc.nasa.gov/images/shuttle-patch-logo.gif> Table of" 404 -
    Read 1569898 lines, successfully parsed 1569003 lines, failed to parse 895 lines



```python
#Q1: Write spark code( using RDD) to find out top 10 requested URLs along with count of number of times 
#they have been requested (This information will help company to find out most popular pages and how frequently they are accessed)

df = spark.createDataFrame(validrecords)

df.registerTempTable("df_tab")
spark.sql("select RequestURL,count(RequestURL) from \
            df_tab Group by RequestURL order by count(RequestURL) desc limit 10").cache().show(truncate = False)


#This output primarily shows image files which will be pulled when a user tries to access a webpage.
#I have written another code in next step to pull only html URLs from data
```

    +---------------------------------------+-----------------+
    |RequestURL                             |count(RequestURL)|
    +---------------------------------------+-----------------+
    |/images/NASA-logosmall.gif             |97384            |
    |/images/KSC-logosmall.gif              |75332            |
    |/images/MOSAIC-logosmall.gif           |67441            |
    |/images/USA-logosmall.gif              |67061            |
    |/images/WORLD-logosmall.gif            |66437            |
    |/images/ksclogo-medium.gif             |62771            |
    |/ksc.html                              |43683            |
    |/history/apollo/images/apollo-logo1.gif|37824            |
    |/images/launch-logo.gif                |35135            |
    |/                                      |30327            |
    +---------------------------------------+-----------------+
    



```python
#Another code snippet to get response for all html requests alone

spark.sql("select RequestURL,count(RequestURL) from \
            df_tab where RequestURL like '%.html' Group by RequestURL order by \
            count(RequestURL) desc limit 10").cache().show(truncate = False)
```

    +-----------------------------------------------+-----------------+
    |RequestURL                                     |count(RequestURL)|
    +-----------------------------------------------+-----------------+
    |/ksc.html                                      |43683            |
    |/shuttle/missions/sts-69/mission-sts-69.html   |24606            |
    |/shuttle/missions/missions.html                |22451            |
    |/software/winvn/winvn.html                     |10345            |
    |/history/history.html                          |10133            |
    |/history/apollo/apollo.html                    |8985             |
    |/shuttle/countdown/liftoff.html                |7865             |
    |/history/apollo/apollo-13/apollo-13.html       |7176             |
    |/shuttle/technology/sts-newsref/stsref-toc.html|6516             |
    |/shuttle/missions/sts-69/images/images.html    |5263             |
    +-----------------------------------------------+-----------------+
    



```python
#Q2: Write spark code to find out top 5 hosts / IP making the request along with count (This information will help company 
#to find out locations where website is popular or to figure out potential DDoS attacks)

df2 = spark.createDataFrame(validrecords)

df2.registerTempTable("df_tab2")

spark.sql("select Host, count(Host) from df_tab2 Group by Host \
          order by count(Host) desc limit 5").cache().show(truncate = False)

#This data has a combination of both "Host names" and/or "IP addresses"
#This code is written using SQL Context. Input data is directly retrieved from Spark datafiles without any Database table.

```

    +--------------------+-----------+
    |Host                |count(Host)|
    +--------------------+-----------+
    |edams.ksc.nasa.gov  |6530       |
    |piweba4y.prodigy.com|4846       |
    |163.206.89.4        |4791       |
    |piweba5y.prodigy.com|4607       |
    |piweba3y.prodigy.com|4416       |
    +--------------------+-----------+
    



```python
#Q3: Write spark code to find out top 5 time frame for high traffic (which day of the week or hour of the day receives 
#peak traffic, this information will help company to manage resources for handling peak traffic load)

from pyspark.sql.functions import desc

df3 = spark.createDataFrame(validrecords)

df3 = df3.withColumn('new_TimeStamp', df3.TimeStamp.substr(1, 13))    

df3 = df3.select("Host", "new_TimeStamp","RequestURL", "HTTPReplyCode", "BytesTransferred", "TimeStamp")  

df3.groupBy("new_TimeStamp").count().sort(desc("count")).limit(5).show()

```

    +-------------+-----+
    |new_TimeStamp|count|
    +-------------+-----+
    |1995-08-31 11| 6297|
    |1995-08-31 10| 6252|
    |1995-08-31 13| 5948|
    |1995-08-30 15| 5889|
    |1995-08-29 15| 5607|
    +-------------+-----+
    



```python
#Same result with SQLContext too
df3.registerTempTable("df_tab3")
spark.sql("select new_TimeStamp, count(new_TimeStamp) from df_tab3 Group by new_TimeStamp \
          order by count(new_TimeStamp) desc limit 5").cache().show(truncate = False)
```

    +-------------+--------------------+
    |new_TimeStamp|count(new_TimeStamp)|
    +-------------+--------------------+
    |1995-08-31 11|6297                |
    |1995-08-31 10|6252                |
    |1995-08-31 13|5948                |
    |1995-08-30 15|5889                |
    |1995-08-29 15|5607                |
    +-------------+--------------------+
    



```python
#Q4: Write spark code to find out 5 time frames of least traffic (which day of the week or hour of the day receives 
#least traffic, this information will help company to do production deployment in that time frame so that less number of users 
#will be affected if some thing goes wrong during deployment)

from pyspark.sql.functions import asc

df4 = spark.createDataFrame(validrecords)

df4 = df4.withColumn('new_TimeStamp', df4.TimeStamp.substr(1, 13))    

df4 = df4.select("Host", "new_TimeStamp","RequestURL", "HTTPReplyCode", "BytesTransferred", "TimeStamp")  

df4.groupBy("new_TimeStamp").count().sort(asc("count")).limit(5).show()
```

    +-------------+-----+
    |new_TimeStamp|count|
    +-------------+-----+
    |1995-08-03 04|   16|
    |1995-08-03 09|   22|
    |1995-08-03 05|   43|
    |1995-08-03 10|   57|
    |1995-08-03 07|   58|
    +-------------+-----+
    



```python
#Same result with SQLContext too
df4.registerTempTable("df_tab4")
spark.sql("select new_TimeStamp, count(new_TimeStamp) from df_tab4 Group by new_TimeStamp \
          order by count(new_TimeStamp) asc limit 5").cache().show(truncate = False)

#This result is quite evident since there was a shut down due to hurricane, there were less requests on 03/08/1995 
#and people started accessing from the time servers started coming up.
```

    +-------------+--------------------+
    |new_TimeStamp|count(new_TimeStamp)|
    +-------------+--------------------+
    |1995-08-03 04|16                  |
    |1995-08-03 09|22                  |
    |1995-08-03 05|43                  |
    |1995-08-03 10|57                  |
    |1995-08-03 07|58                  |
    +-------------+--------------------+
    



```python
#So, I have analysed the logs again after excluding 03/08/1995 from TimeStamp
spark.sql("select new_TimeStamp, count(new_TimeStamp) from df_tab4 where new_TimeStamp not like '1995-08-03%' \
          Group by new_TimeStamp \
          order by count(new_TimeStamp) asc limit 5").cache().show(truncate = False)

#This gives an unbiased response :-)
```

    +-------------+--------------------+
    |new_TimeStamp|count(new_TimeStamp)|
    +-------------+--------------------+
    |1995-08-13 02|266                 |
    |1995-08-26 05|423                 |
    |1995-08-13 03|445                 |
    |1995-08-26 06|479                 |
    |1995-08-20 06|502                 |
    +-------------+--------------------+
    



```python
#Q5: Write spark code to find out unique HTTP codes returned by the server along with count (this information is helpful 
#for devops team to find out how many requests are failing so that appropriate action can be taken to fix the issue)

httpcode = (validrecords
                       .map(lambda code: (code.HTTPReplyCode, 1))
                       .reduceByKey(lambda x, y : x + y)
                       .cache())
httpcodelist = httpcode.take(1000)
print 'Found %d Unique HTTP Response codes' % len(httpcodelist)
print 'Count by each code: %s' % httpcodelist
```

    Found 7 Unique HTTP Response codes
    Count by each code: [(200, 1398207), (302, 26437), (304, 134138), (403, 171), (404, 10020), (501, 27), (500, 3)]



```python
#In the above response, we can see that HTTP Code: 200 is received by most of the requests. 
#Code 200 - Request was fulfilled
#Code 302 - The data requested actually resides under a different URL, however, the redirection may be altered on occasion.
#Code 304 - f the client has done a conditional GET and access is allowed, but the document has not been modified since the 
#date and time specified 
#Code 403 - The request is for something forbidden. Authorization will not help.
#Code 404 - The server has not found anything matching the URI given
#Code 501 - The server does not support the facility required.
#Code 500 - The server encountered an unexpected condition which prevented it from fulfilling the request.

#This is definitely a very useful information for developers.
```


```python
#I have used different types of approaches to answer each question.
#So far, SQL Context got executed faster compared to RDD and Spark Dataframes.
```

from pyspark import SparkContext
from pyspark.sql import SQLContext
import re

def parse_lines(line):
    relevant = line.split("]:")[1]
    # Template line from ssh log
    s = 'Invalid user (.*) from (.*) port (.*)'
    re_m = re.search(s, relevant)
    # Example tuple: ('usr', [('11.111.111.11', 222)]) 
    return (re_m[1], [(re_m[2], int(re_m[3]))])

def count_occurances(line):
    (key, value) = line
    # Example tuple: ('usr', (1, [('11.111.111.11', 222)]))
    return (key, (len(value), value))

def remove_ip_list(line):
    (key, value) = line
    return (key, value[0])

sc = SparkContext("local", "count-names")
lines = sc.textFile("sshd_invalid.txt")

parsed = lines.map(parse_lines)
reduced = parsed.reduceByKey(lambda a,b: a+b)
counted = reduced.map(count_occurances)
final = counted.sortBy(lambda line: line[1][0], ascending=False)
final.saveAsTextFile("output")

# remove extra for easier analysis
simple = final.map(remove_ip_list)

sqlc = SQLContext(sc)
df = sqlc.createDataFrame(simple, ["usernames", "count"])
# if data is very large, save in part files with below command instead
# df.write.csv("usernames_count.csv", header=True)
# The below command needs everything in RAM
# in my case, data is small so this is ok
df.toPandas().to_csv("usernames_count.csv", index=False)

# PySpark-Cheat-Sheet

### Create basic DataFrame
```
integerDF = (spark.createDataFrame([
  (1, 2),
  (3, 4),
  (5, 6)
], ["col1", "col2"]))
```

### Add column
```
newDF = (originalDF
  .withColumn("newcolumnname", col("id") + 1 )
)
```

### Drop Column
```
dedupedDF = dupedWithColsDF.drop("lcFirstName", "lcMiddleName", "lcLastName", "ssnNums")
`` 

### Replace/manipulate value on column
```
from pyspark.sql.functions import col, lower, regexp_replace

dupedWithColsDF = dupedDF.select(
                                    lower(col("firstName")).alias("lcFirstName"),
                                    lower(col("middleName")).alias("lcMiddleName"),
                                    lower(col("lastName")).alias("lcLastName"),
                                    regexp_replace(col("ssn"),'-','').alias("ssnNums")
                                ).dropDuplicates(["lcFirstName","lcMiddleName","lcLastName","ssnNums"])
```


### Normalize values between 0 and 1
```
 from pyspark.sql.functions import col, max, min

colMin = integerDF.select(min("id")).first()[0]
colMax = integerDF.select(max("id")).first()[0]

normalizedIntegerDF = (integerDF
  .withColumn("normalizedValue", (col("id") - colMin) / (colMax - colMin) )
)

display(normalizedIntegerDF)
```

### Imputing Null or Missing Data
Drop any records that have null values.
```
corruptDroppedDF = corruptDF.dropna("any")
```

Impute values with the mean. Update when null
```
corruptImputedDF = corruptDF.na.fill({"temperature": 68, "wind": 6})
```

### Deduplicating Data - Distinct(+/-)
```
duplicateDedupedDF = duplicateDF.dropDuplicates(["id", "favorite_color"])
```


### Custom Transformations with User Defined Functions
```
from pyspark.sql.types import StringType


def manual_split(x):
  return x.split("e")

manualSplitPythonUDF = spark.udf.register("manualSplitSQLUDF", manual_split, StringType())

randomAugmentedDF = randomDF.select("*", manualSplitPythonUDF("hash").alias("augmented_col"))
```

### Register DataFrame to access it with SQL
```
randomDF.createOrReplaceTempView("randomTable")
```
```
%sql
SELECT id,
  hash,
  manualSplitSQLUDF(hash) as augmented_col
FROM
  randomTable
```

### Join Two DataFrames by column name
```
pageviewsEnhancedDF = pageviewsDF.join(labelsDF, "dow")
```


### Group by and Sum
```
from pyspark.sql.functions import col

aggregatedDowDF = (pageviewsEnhancedDF
  .groupBy(col("dow"), col("longName"), col("abbreviated"), col("shortName"))  
  .sum("requests")                                             
  .withColumnRenamed("sum(requests)", "Requests")
  .orderBy(col("dow"))
)
```

### Save DataFrame HIVE table (SQL)
```
df.write.mode("OVERWRITE").saveAsTable("myTableManaged")
#or
df.write.mode("OVERWRITE").option('path', 'home/dbel/myTableUnmanaged').saveAsTable("myTableUnmanaged")

```
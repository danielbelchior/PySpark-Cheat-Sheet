# PySpark-Cheat-Sheet

### Add column
```
newDF = (originalDF
  .withColumn("newcolumnname", col("id") + 1 )
)
```

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


// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)



// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")
sc.defaultParallelism

// COMMAND ----------

spark.catalog.clearCache()
val fileName = "/FileStore/tables/pageviews_by_second.tsv"

//val df2 = spark.read
  //.parquet(parquetDir)
//.repartition(1)
   // .coalesce(2)
//.groupBy("site").sum()

val df = spark.read.option("sep", "\t")
  .option("header", "true").schema(schema)
  .csv(fileName).repartition(16).groupBy($"timestamp").sum()
df.explain
print(df.count())

import org.apache.spark.sql.functions.spark_partition_id

print(df.groupBy(spark_partition_id).count.show)

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC ##Coalesce 
// MAGIC 6 - 15s  
// MAGIC 5 - 14s  
// MAGIC 4 - 15s  
// MAGIC 3 - 15s  
// MAGIC 2 - 14s  
// MAGIC 1 - 15s

// COMMAND ----------

// MAGIC %md
// MAGIC ## Repartition
// MAGIC Dla 2000 partycji jest to średnio 2.5 minuty, dla 4000 liczba ta rośnie dwa razy i trwa średnio 3.5min. Dla 200 partycji mam wynik znacznie niższy i są tylko tylko 18s. Dla 96 jest to średnio 16 s. Dla 16 mamy średnio 6s. Bardzo podobne wyniki są dla 9 partycji. Dla 7 oraz 1 są to wyniki w granicach 2-3s. Dla 1 partycji zmienił się również plan wykonania. **Wnioski**:  
// MAGIC - gdy liczba partycji nie jest jest równa wielokrotnością liczby slotów to działanie operacji się wydłuża,  
// MAGIC - dla jednej partycji czas działania jest krótszy a liczba jobów dla tego konkretnego przypadku dwa razy mniejsza,
// MAGIC - znacznie zawyżona liczba partycji (2000,4000) powoduje, że dana partycja zawiera mało danych a wykonanie działania na dataframe wymaga dużych zasobów co powoduje, że czas działania operacji jest znacznie większy. Widać to szczególnie na małych zbiorach danych, kazda partycja zawiera kilka Kb przez co przenoszenie wyników na poszczególne partycje trwa stosunkowo długo,
// MAGIC - wyniki dla coalesce dla badanej liczby partycji praktycznie sie nie różnią, może to być spowodowane tym, że po tej operacji nie mamy gwarancji, że rozmiary naszych partycji będą podobne, a spark zbudowany został tak, żeby działać na podobnych (w sensie wielkościowym) partycjach.
// MAGIC - dla coalesce w planie wykonanie nie ma shuffle
// MAGIC - plan działania operacji w Spark UI jest taki sam w przypadku wywołania obu akcji.

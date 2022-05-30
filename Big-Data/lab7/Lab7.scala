// Databricks notebook source
// MAGIC %md
// MAGIC Indeksowanie to stosunkowo nowa funkcja w Hive. Przy petabajtach danych, które należy przeanalizować, wykonywanie zapytań do tabel Hive zawierających miliony rekordów i setki kolumn staje się czasochłonne. Indeksowanie tabeli pomaga w szybszym wykonywaniu dowolnej operacji. Najpierw sprawdzany jest indeks kolumny, a następnie operacja jest wykonywana tylko na tej kolumnie. Bez indeksu zapytania zawierające filtrowanie z klauzulą „WHERE” ładowałyby całą tabelę, a następnie przetwarzały wszystkie wiersze. Indeksowanie w Hive jest obecne tylko dla formatu pliku ORC, ponieważ ma wbudowany indeks. 
// MAGIC Aby przyśpieszyć przeszukiwanie możemy stworzyć indeks przy użyciu zapytania CREATE INDEX...
// MAGIC 
// MAGIC Indeksowanie jest również dobrą alternatywą dla partycjonowania, gdy partycje logiczne byłyby w rzeczywistości zbyt liczne i małe, aby były użyteczne. Indeksowanie może pomóc w oczyszczeniu niektórych bloków z tabeli jako danych wejściowych dla zadania MapReduce. Nie wszystkie zapytania mogą korzystać z indeksu — składnia EXPLAIN i Hive mogą służyć do określania, czy dane zapytanie jest wspomagane przez indeks.
// MAGIC 
// MAGIC Indeksy w Hive, podobnie jak te w relacyjnych bazach danych, należy dokładnie ocenić. Utrzymanie indeksu wymaga dodatkowego miejsca na dysku, a budowanie indeksu wiąże się z kosztami przetwarzania.

// COMMAND ----------

spark.catalog.listDatabases().show()
spark.sql("create database if not exists Sample")

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val df = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
df.write.mode("overwrite").saveAsTable("sample.names")
val filePath_2 = "dbfs:/FileStore/tables/Files/movies.csv"
val df_2 = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


df_2.write.mode("overwrite").saveAsTable("sample.movies")


// COMMAND ----------

spark.catalog.listDatabases().show()
spark.catalog.listTables("sample").show

// COMMAND ----------

import org.apache.spark.sql.types._


def drop_content(database: String){
  
  val tables = spark.catalog.listTables(s"$database")
  val names=tables.select("name").as[String].collect.toList
  var i = List()
  for( i <- names){
    spark.sql(s"DELETE FROM $database.$i")
  }
  
}


// COMMAND ----------

drop_content("sample")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM sample.names

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/jars

// COMMAND ----------

// MAGIC %md
// MAGIC org.example is a library packed into a jar file. There is a test function of downloading a file from the internet and doing some transformations on it.

// COMMAND ----------


import org.example.lab7
val args =  Array.empty[String]
lab7.main(args)

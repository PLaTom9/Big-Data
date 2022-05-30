// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._

val filePath = "dbfs:/FileStore/tables/Files/names.csv"

val start_Time = System.currentTimeMillis()

val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)    


//Add a column where you can calculate your height in feet
val namesDf2 = namesDf.withColumn("HeightFeet",round(($"Height")*0.0328,2))


//Answer the question what is the most popular name?
val most_common_name = namesDf2.select("name").withColumn("FName", split(col("name"), " ").getItem(0)).groupBy("FName").count().sort($"count".desc).select("FName").first().getString(0)
println(most_common_name)


//Add a column and count the age of the actors
val namesDf2_with_bday= namesDf2.select($"*",
    when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull,
           to_date(col("date_of_birth"),"yyyy-MM-dd"))
    .when(to_date(col("date_of_birth"),"yyyy MM dd").isNotNull,
           to_date(col("date_of_birth"),"yyyy MM dd"))
    . when(to_date(col("date_of_birth"),"MM/dd/yyyy").isNotNull,
           to_date(col("date_of_birth"),"MM/dd/yyyy"))
    .when(to_date(col("date_of_birth"),"yyyy MMMM dd").isNotNull,
           to_date(col("date_of_birth"),"yyyy MMMM dd"))
    .when(to_date(col("date_of_birth"),"dd.MM.yyyy").isNotNull,
           to_date(col("date_of_birth"),"dd.MM.yyyy"))
    .otherwise(null).as("Formated_Date_Birth")
  )
val namesDf2_with_dday = namesDf2_with_bday.select($"*",
    when(to_date(col("date_of_death"),"yyyy-MM-dd").isNotNull,
           to_date(col("date_of_death"),"yyyy-MM-dd"))
    .when(to_date(col("date_of_death"),"yyyy MM dd").isNotNull,
           to_date(col("date_of_death"),"yyyy MM dd"))
    . when(to_date(col("date_of_death"),"MM/dd/yyyy").isNotNull,
           to_date(col("date_of_death"),"MM/dd/yyyy"))
    .when(to_date(col("date_of_death"),"yyyy MMMM dd").isNotNull,
           to_date(col("date_of_death"),"yyyy MMMM dd"))
    .when(to_date(col("date_of_death"),"dd.MM.yyyy").isNotNull,
           to_date(col("date_of_death"),"dd.MM.yyyy"))
    .otherwise(null).as("Formated_Date_Death")
  )
val namesDf3 = namesDf2_with_dday.withColumn("Age",when($"Formated_Date_Death".isNull && year($"Formated_Date_birth")>1930,round(months_between(
        col("Formated_Date_Birth"),current_date(),true).divide(12),0)*(-1)).otherwise(round(months_between(
        col("Formated_Date_Birth"),col("Formated_Date_Death"),true).divide(12),0)*(-1)))


//Remove columns (bio, death_details)
val namesDf4 = namesDf3.drop(namesDf3("bio")).drop(namesDf3("death_details"))


//Rename columns - add capitalization and remove _
val namesDf5= namesDf4.select(namesDf4.columns.map(x => col(x).as(x.toLowerCase.split('_').map(_.capitalize).mkString(""))): _*)


//Sort the dataframe by name in ascending order
val namesDf6 = namesDf5.select("*").sort($"Name".asc)

val end_Time = System.currentTimeMillis()

//Add a column with the notebook's runtime value in epoch format
val namesDf_withTime = namesDf6.withColumn("EditingTime",lit((end_Time-start_Time)/1e3d) )
display(namesDf_withTime)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_extract

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"

val start_Time_2 = System.currentTimeMillis()
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


//Add a column that tells you how many years have passed since the movie was published
val moviesDf2 = moviesDf.select($"*",
    when(to_date(col("date_published"),"yyyy-MM-dd").isNotNull,
           to_date(col("date_published"),"yyyy-MM-dd"))
    .when(to_date(col("date_published"),"yyyy MM dd").isNotNull,
           to_date(col("date_published"),"yyyy MM dd"))
    . when(to_date(col("date_published"),"MM/dd/yyyy").isNotNull,
           to_date(col("date_published"),"MM/dd/yyyy"))
    .when(to_date(col("date_published"),"yyyy MMMM dd").isNotNull,
           to_date(col("date_published"),"yyyy MMMM dd"))
    .when(to_date(col("date_published"),"dd.MM.yyyy").isNotNull,
           to_date(col("date_published"),"dd.MM.yyyy"))
    .otherwise(null).as("Formated_Date_Published")
  ).withColumn("Time_from_publication",round(months_between(
        col("Formated_Date_Published"),current_date(),true).divide(12),0)*(-1))


//Add a column that will show the movie budget as a numeric value (need to remove currency signs)
//Remove the lines from the dataframe where the values are null
val moviesDf3 = moviesDf2.withColumn("budget_num", regexp_extract($"budget", "\\d+", 0).cast("integer")).na.drop("any")


val end_Time_2 = System.currentTimeMillis()

//Add a column with the notebook's runtime value in epoch format
val moviesDf_withTime = moviesDf3.withColumn("EditingTime",lit((end_Time_2-start_Time_2)/1e3d) )
display(moviesDf_withTime)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.functions._
import spark.implicits._
import scala.util.control.Breaks._
import org.apache.spark.sql.types._

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"

val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
val start_Time_3 = System.currentTimeMillis()


//For each of the following enumerations, do not consider nulls
val ratingsDf2 = ratingsDf.na.drop("any")


//Add new columns and calculate the mean and medians for the vote values (1 to 10)
var votesColums:Array[Any] = Array()
val i= 0
for( i<-1 to 10){
  votesColums = votesColums :+ col("votes_".concat(i.toString))*i
}
val averageFunc = votesColums.foldLeft(lit(0)){(x, y) => x+y}

val ratingsDf3 = ratingsDf2.withColumn("Result(Avg)", round(averageFunc/$"total_votes",1))

def median = udf((len:Int,row: Row) => {
  var center= 0
  if(len%2==1) center=len/2+1 else center=(len/2+len/2+1)/2
  val i=0
  var sum=0
  var median = 0
  breakable{
  for( i<-0 to 9){
    sum=sum+row.getInt(i)
    if(sum>center){
      median=i
      break
    }
  }
  }
  median+1
})

val ratingsDf4 = ratingsDf3.withColumn("Result(Median)", median(col("total_votes"),struct($"votes_1", $"votes_2", $"votes_3", $"votes_4", $"votes_5", $"votes_6", $"votes_7", $"votes_8", $"votes_9", $"votes_10")))


//For each mean and median, calculate the difference between weighted_average_vote
val ratingsDf5 = ratingsDf4.withColumn("diffAvg",round(abs($"mean_vote"-$"weighted_average_vote"),1)).withColumn("diffMed",round(abs($"median_vote"-$"weighted_average_vote"),1))


//Who gives the best boys or girls grades for the whole set
val data = ratingsDf2.select(mean("females_allages_avg_vote") , mean("males_allages_avg_vote"))
if(data.first().getDouble(0) > data.first().getDouble(1) )
println("Women give better grades.")
else
println("Mans give better grades.")


//For one of the columns, change the data type to long
val ratingsDf6 = ratingsDf5.withColumn("allgenders_45age_votes",col("allgenders_45age_votes").cast(LongType))


val end_Time_3 = System.currentTimeMillis()
//Add a column with the notebook's runtime value in epoch format
val ratingsDf_withTime = ratingsDf6.withColumn("EditingTime",lit((end_Time_3-start_Time_3)/1e3d) )
display(ratingsDf_withTime)

// COMMAND ----------

// MAGIC %md
// MAGIC SPARK UI\
// MAGIC Jobs Tab - wyświetla podsumowanie wszystkich zadań w aplikacji Spark, wraz ze szczegółami dla każdego zadania. Elementy wyświetlane to np. User, Total uptime,Scheduling mode,Number of jobs per status\
// MAGIC Stages Tab - wyświetla stronę podsumowaia, która pokazuje biężący stan wszystkich etapów każdego zadania.Na początku strony znajduje się podsumowanie z liczbą wszystkich etapów według statusu (aktywny, oczekujący, zakończony, pominięty, nieudany)\
// MAGIC Storage Tab - wyświetla utrwalone dyski RDD (np rozmiary, partycje) i istniejące DataFrame\
// MAGIC Environment Tab - wyświetla informacje o róznych zmiennych środowiskowych i konfiguracyjnych\
// MAGIC Executors Tab - wyświetla informacje o utworzonych executorach, np wykorzystywana pamięć, zużycie dysku, obsługiwane zadania\
// MAGIC SQL Tab - wyświetla informację o zapytaniach SQL (o ile występują) takie jak czas trwania, zadania oraz plany fizyczne i logiczne\
// MAGIC Structured Streaming Tab - jak odpalimy Structured Streaming jobs w trybie micro-batch, to za pomocą tej tablicy dostajemy kilka krótkich statystyk dotyczących uruchomionych i zakończonych zapytań\
// MAGIC Streaming (DStreams) Tab - wyświetla opóźnienie planowania i czas przetwarzania dla każdej micro-batch  w strumieniu danych\
// MAGIC JDBC/ODBC Server Tab - Możemy zobaczyć tę kartę, gdy Spark działa jako rozproszony silnik SQL. Pokazuje informacje o sesjach i przesłanych operacjach SQL (czas uruchomienia, czas pracy)\

// COMMAND ----------

//Add groupBy transformations to one of the Dataframes and compare what the execution plan looks like
ratingsDf6.select($"*").explain()
ratingsDf6.select($"*").groupBy("total_votes").count().explain()
//Without groupBy (), the plan consisted of only 3 activities, after adding groupBy there are twice as many.

// COMMAND ----------

//Retrieve data from at least one table from SQL Server. Check what are the options for writing data using the jdbc connector.

val jdbcDF = sqlContext.read
      .format("jdbc")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT * FROM sys.columns) temp")
      .option("user", "sqladmin")
      .option("password", "$3bFHs56&o123$")
      .load()

display(jdbcDF)

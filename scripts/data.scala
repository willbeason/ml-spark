import org.apache.spark.SparkContext


val sc = new SparkContext("local", "ML Project")


val data = sc.textFile("data/Crimes_-_2001_to_present.csv")

data.count()


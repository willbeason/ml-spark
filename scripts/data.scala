import java.io.{File, FileWriter}

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark.SparkContext

import scala.util.Try


val sc = new SparkContext("local", "ML Project")


val lines = sc.textFile("data/Crimes_-_2001_to_present.csv")

case class Crime(
    id: String, // 0
    caseNumber: String, // 1
    date: String, // 2
    block: String, // 3
    iucr: String, // 4
    primaryType: String, // 5
    description: String, // 6
    locationDescription: String, // 7
    arrest: Boolean, // 8
    domestic: Boolean, // 9
    beat: String, // 10
    district: String, // 11
    ward: String, // 12
    communityArea: String, // 13
    fbiCode: String, // 14
    xCoordinate: String, // 15
    yCoordinate: String, // 16
    year: Int, // 17
    updatedOn: String, // 18
    latitude: Double, // 19
    longitude: Double, // 20
    location: String // 21
) {}

object Crimes {
  def from(s: String): Option[Crime] = {
    val splits = s.split(",")
    if (splits.length < 21) None
    else {
      Try(
        Crime(
          splits(0),
          splits(1),
          splits(2),
          splits(3),
          splits(4),
          splits(5),
          splits(6),
          splits(7),
          splits(8).toBoolean,
          splits(9).toBoolean,
          splits(10),
          splits(11),
          splits(12),
          splits(13),
          splits(14),
          splits(15),
          splits(16),
          splits(17).toInt,
          splits(18),
          splits(19).toDouble,
          splits(20).toDouble,
          splits(21)
        )
      ).toOption
    }
  }
}

val header = lines.first()
val data = lines.filter(_ != header).map(Crimes.from).collect {
  case Some(crime) => crime
}

data.count()

data.take(1).foreach(println)

val fw = new FileWriter(new File("data/all-crimes-locations-s.csv"))
val printer = new CSVPrinter(fw, CSVFormat.RFC4180.withHeader("latitude", "longitude"))

data.map {
  crime => s"${crime.latitude},${crime.longitude}"
}.toLocalIterator.foreach {
  record =>
    printer.printRecord(record)
}

fw.close()

import java.io.{File, FileWriter}

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}

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
) {
  def toLabeledPoint: Option[LabeledPoint] = {
    Try(LabeledPoint(
      if(arrest) 1.0 else 0.0,
      Vectors.dense(
        district.toDouble,
        latitude,
        longitude
      )
    )).toOption
  }
}

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

//data.take(1).foreach(println)
//
//val fw = new FileWriter(new File("data/all-crimes-locations-s.csv"))
//val printer = new CSVPrinter(fw, CSVFormat.RFC4180.withHeader("latitude", "longitude"))
//
//data.toLocalIterator.foreach {
//  crime =>
//    printer.printRecord(crime.latitude.toString, crime.longitude.toString)
//}

//fw.close()


val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// The defaultParams for Classification use LogLoss by default.
val boostingStrategy = BoostingStrategy.defaultParams("Classification")
boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
boostingStrategy.treeStrategy.numClasses = 2
boostingStrategy.treeStrategy.maxDepth = 5
// Empty categoricalFeaturesInfo indicates all features are continuous.
boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

val model = GradientBoostedTrees.train(trainingData.flatMap(_.toLabeledPoint), boostingStrategy)

// Evaluate model on test instances and compute test error
val labelAndPreds = testData.flatMap(_.toLabeledPoint).map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification GBT model:\n" + model.toDebugString)


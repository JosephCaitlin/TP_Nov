package com.fakir.samples

object Exercice1_RDD {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SparkSession

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1 : Lire le fichier "films.csv" sous forme de RDD[String].
    val rdd: RDD[String] = sparkSession.sparkContext.textFile("data/donnees.csv")

    // Question 2 : Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    val leonardo = rdd.filter(elem => elem.contains("Di Caprio")).count()
    println(leonardo)

    // Question 3 : Quelle est la moyenne des notes des films de Di Caprio ?
    val moyenne_leonardo = rdd.filter(elem => elem.contains("Di Caprio")).map(item => (item.split(";")(2).toDouble)).mean()
    println(moyenne_leonardo)

    // Question 4 :  Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?
    val vues_leonardo = rdd.filter(elem => elem.contains("Di Caprio")).map(item => (item.split(";")(1).toDouble)).sum()
    val vues_totales = rdd.map(item => (item.split(";")(1).toDouble)).sum()
    val pourcentage = (vues_leonardo / vues_totales) * 100
    println(pourcentage)

    // Question 5 : Quelle est la moyenne des notes par film dans cet échantillon ?
    //    1. Pour cette question, il faut utiliser les Pair-RDD
  }
}

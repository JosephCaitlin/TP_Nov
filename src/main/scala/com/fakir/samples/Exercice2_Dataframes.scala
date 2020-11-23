package com.fakir.samples


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Exercice2_Dataframes {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org") setLevel Level.OFF
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    //Question 1 : Lire le fichier "films.csv" avec la commande suivante :
    //
    val df: DataFrame = sparkSession.read.option("delimiter", ";").option("inferSchema", true).option("header", false).csv("data/donnees.csv")
    //Question 2 : Nommez les colonnes comme suit : nom_film, nombre_vues, note_film, acteur_principal
    val df= df.withColumnRenamed("_c0", "nom_film")
    val df= df.withColumnRenamed("_c1", "nombre_vues")
    val df= df.withColumnRenamed("_c2", "note_film")
    val df= df.withColumnRenamed("_c3", "acteur_principal")
    df.show


    // Question 3 : Refaire les questions 2, 3, 4 et 5 en utilisant les DataFrames.
    //2. Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    //val leonardo = df.filter(col("_c3") =="Di Caprio").count()
    //println(leonardo)

    //3. Quelle est la moyenne des notes des films de Di Caprio ?
    //val moyenne_leonardo = df.groupBy("_c3").mean("_c2").filter("_c3")
    //moyenne_leonardo.show

    //4.Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?
    //val vues_leonardo = df.groupBy("_c3").sum("_c1").filter("_c3") == "Di Caprio")
    //val vues_totales = df.groupBy().sum("_c1")
    //val pourcentage = (vues_leonardo / vues_totales) * 100
    //println(pourcentage)

// Question 4 :Créer une nouvelle colonne dans ce DataFrame, "pourcentage de vues", contenant le pourcentage de vues pour chaque film (combien de fois le film a-t-il été vu par rapport aux vues globales ?)
}
}

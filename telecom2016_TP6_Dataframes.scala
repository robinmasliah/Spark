
object Cells {
  import org.apache.spark.sql.SparkSession
  
  //def TODO =  // ceci est juste un marquer d'une valeur que vous devez remplacer dans les lignes qui suivent
  
  // on cree une session Spark avec un nom particulier pour la retrouver plus facilement dans le SparkUI
  val mySession = SparkSession
    .builder()
    .config(new SparkConf()
               .setAppName("openSparkSession"))//rajouter comme parametre le nom de votre application
    .getOrCreate()

  /* ... new cell ... */

  // TODO: verifier/modifier le chemin vers les fichiers de donnees: 
  val path="notebooks/telecom2016/data/" 

  /* ... new cell ... */

  // Lecture des donnes dans des DataFrames en utilisant la premier ligne du fichier comme nom des colonnes
  val lieux /*:DataFrame*/ = mySession.read.option("header","true").format("com.databricks.spark.csv")
                                        .load(path + "/lieux_2015.csv")
                                        .cache
  val caracteristiques = mySession.read.option("header","true").format("com.databricks.spark.csv")
                                         .load(path + "/caracteristiques_2015.csv")
                                         .cache
  val usagers = mySession.read.option("header","true").format("com.databricks.spark.csv")
                                        .load(path + "/usagers_2015.csv")
                                        .cache
  val vehicules = mySession.read.option("header","true").format("com.databricks.spark.csv")
                                        .load(path + "/vehicules_2015.csv")
                                        .cache 

  /* ... new cell ... */

  // Un DataFrame est un DataSet[Row] ou par default chaque ligne est une "String"
  lieux.printSchema

  /* ... new cell ... */

  // afficher le schema des 3 autres Dataframes
  vehicules.printSchema
  usagers.printSchema

  /* ... new cell ... */

  // on peut utiliser la methode show pour afficher le contenu d'un DataFrame
  usagers.show(4) // affiche 4 lignes du dataframe usagers

  /* ... new cell ... */

  print {
    s"Le jeu de donnees contiens ${lieux.count} accidents qui ont implique ${vehicules.count} vehicules, ${usagers.count} personnes. Le nombre des caracteristiques des accidents: ${TODO}"
  }

  /* ... new cell ... */

  //Afficher le nombre des victimes groupees par la gravite de la blessure. Utiliser un groupBy sur le dataframe usagers
  val victimes = usagers.groupBy("grav").count
  victimes.show // pour afficher le contenu

  /* ... new cell ... */

  // Hmm, les valeurs numeriques ne sont pas trop comprehensible, nous pouvons les decoder en utilisant 
  victimes.map{
    row => {
      val mapping = Map(
        "1" -> "Indemne",
        "2" -> "Tué",
        "3" -> "Blessé hospitalisé",
        "4" -> "Blessé léger"
      )
      mapping(row.getAs[String]("grav")) +" : "+ row.getAs[String]("count")
    }
  }.show(10,false)

  /* ... new cell ... */

  usagers.filter($"grav" === 3).count // filtrer sur la gravite === 3

  /* ... new cell ... */

  import org.apache.spark.sql.types._
  
  // d'abord on doit creer une nouvelle colonne an_nais_int = pour convertir la colonne an_nais en entier
  val usagers1 = usagers.withColumn("an_nais_int", col("an_nais").cast(IntegerType))

  /* ... new cell ... */

  // puis on defini une User Defined Function qui calcule l'age a partir de la date de naissance
  val computeAge = udf {(an_nais: Int) =>  2016 - an_nais}
  
  //et on rajoute une nouvelle colonne qui sera peuplee avec le resultat de l'application de l'udf computeAge sur la colonne "an_nais"
  val usagersAge = usagers1.withColumn("age", computeAge(usagers1("an_nais_int") ))
  
  usagersAge.show()

  /* ... new cell ... */

  // Afficher la repartition des victimes par age, triee par l'age des victimes
  
  usagersAge.groupBy("age").count().sort(desc("count")).show(10) //affichez uniquement les 10 premieres lignes

  /* ... new cell ... */

  val accidents = usagers
      .filter($"grav" === 2 ) // on filtre les victimes
      .groupBy("Num_Acc") // on groupe sur l'id de l'accident
      .count() // on compte le nombre des victimes
      .sort(desc("count"))// on trie par le nombre des victimes
      .limit(3)// on garde uniquement les 3 accidents les plus meurtirers
      .withColumnRenamed("count","nb_victimes")
      .join(caracteristiques, usagers("Num_acc")=== caracteristiques("Num_Acc"))// on fait le jointure avec les caracteristiques
      .join(lieux, usagers("Num_acc")=== lieux("Num_Acc"))// on fait le jointure avec les lieux
      .join(vehicules, usagers("Num_Acc")=== vehicules("Num_Acc")) // on fait la jointure avec les vehicules
      .groupBy(caracteristiques("an"),caracteristiques("mois"),caracteristiques("jour"),caracteristiques("adr"),caracteristiques("dep"), caracteristiques("atm"),$"nb_victimes") // on groupe sur les colonnes qui nous interessent
      .count().withColumnRenamed("count","nb_vehicules") // on compte le nombre des vehicules
  accidents.show

  /* ... new cell ... */

  // On commence par definir une case class qui va definir le types des lignes de notre jeu de donnes
  case class Caracteristiques(NumAcc: String, latitude: Double, longitude:Double,adr: String, colType: Int)

  /* ... new cell ... */

  
  //On doit transformer chaque ligne (Row) dans un objet Caracteristiques
  // on defini une fonction de mapping qui prends un objet de type Row et retourne une Caracteristique
  def mapRow(r:Row): Caracteristiques = {
    Caracteristiques(
      r.getAs[String]("Num_Acc"),
      r.getAs[String]("lat").toDouble/100000, 
      r.getAs[String]("long").toDouble/100000, 
      r.getAs[String]("adr"), 
      r.getAs[String]("col").toInt)
  }

  /* ... new cell ... */

  // On transforme le dataframe caracteristique (Dataset[Row]) dans un DataSet caractDS (Dataset[Caracteristiques])
  val caractDS/*:Dataset[Caracteristiques]*/ = caracteristiques.filter(
              x => {
                try{
                  mapRow(x)
                  true
                }catch {
                  case e: Exception => false
                }
              }).map{ mapRow }

  /* ... new cell ... */

  // On defnit une methode qui pour une Caracteristique retourne true si la Caracteristique decrit un accident proche d'un endroit particulier (lat:48.71 et long:2.24)
  def nearPlace(c:Caracteristiques): Boolean = {
     ( Math.abs(c.latitude - 48.71)< 0.06 && Math.abs(c.longitude - 2.24) < 0.06) //remplacez les TODO par la latitude/longitude de votre choix , ex pour Palaiseau : lat=48.71, long=2.24 
  }
  nearPlace(Caracteristiques("test",48.71,2.24," my home", 0))// on test la methode en l'appliquant sur une Caracteristique

  /* ... new cell ... */

  // Trouver un accident proche
  val points = caractDS.filter(nearPlace _).take(1)

  /* ... new cell ... */

  // Trouver la liste des accidents proche de ma maison
  val points = caractDS.filter(nearPlace(_)).map(c=> (c.latitude,c.longitude,c.colType,c.NumAcc)).collect.toSeq

  /* ... new cell ... */

  // Afficher les accidents proche sur une carte via un widget du notebook
  val w = GeoPointsChart(points, maxPoints=500)
  w

  /* ... new cell ... */

  // Via le widget PivotChart on peux faire des analyses comme dans un tableur
  // TODO: faites des aggregations/heatmaps/charts en utilisant le widget PivotChart
  PivotChart(caracteristiques,maxPoints=300)

  /* ... new cell ... */
}
                  
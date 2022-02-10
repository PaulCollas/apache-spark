################## INSTANCIATION ###############################
# Instanciation de l'Objet SparkSession
if __name__ == '__main__':
    # Import des dépendances de pysparks
    from pyspark.sql.functions import * 
    from pyspark.sql.types import * 
    from pyspark.sql import SparkSession

    # Import des dépendances / Gere le problème de "col"
    from pyspark.sql.functions import col
    from pyspark.sql.functions import avg

    # Import de sys (pour gérer les arguments de spark-submit)
    import sys

    # Instanciation de l'object spark
    spark = SparkSession.builder \
        .master("local") \
        .appName("Apple") \
        .getOrCreate()

    # Instanciation des produits
    # appleProducts = spark.read.format("csv").option("header", "true").option("header", "true").option("inferSchema", "true").load("./ApplePrices.csv")
    appleProducts = spark.read.format("csv").option("header", "true").option("header", "true").option("inferSchema", "true").load(sys.argv[1])
    
    # Instanciation du tableau de conversion
    # conversion= spark.read.format("csv").option("header", "true").option("header", "true").option("inferSchema", "true").load("./CurrencyConversion.csv")
    conversion= spark.read.format("csv").option("header", "true").option("header", "true").option("inferSchema", "true").load(sys.argv[2])




################## EXERCICE 1 ###############################
    # Convertir les prix des différents produits en dollar (USD)
    appleProducts1 = appleProducts.join(conversion, appleProducts.Currency == conversion.ISO_4217).withColumn("PriceInDollars", col("Price")/col("Dollar_To_Curr_Ratio"))
    appleProducts1 = appleProducts1.select("Model_name","Price","Country","Currency","PriceInDollars")
    



################## EXERCICE 2 ###############################
    # Etablir la moyenne des prix des produits de chaque pays et de comparer à l'équivalent du prix classique US.
    # Ajout d'une colonne du pourcentage d'écart entre la moyenne des produits aux US et la moyenne des produits vendus dans le pays.
    # Trier le DF par ordre croissant
    # Enregistrer ce DF en csv (moyennePrix.csv)

    # 1e version - Division de tout les produits avec l'écarts
    # appleProducts1 = appleProducts1.withColumn("écart",(col("avg(PriceInDollars)") * 100 / appleProducts1.select("avg(PriceInDollars)").where(col("Country").contains("United States")).first()[0] - 100).cast('int')).sort(col("avg(PriceInDollars)").desc()).write.mode("append").format("csv").option("sep",";").save("./")

    # 2e version 

    dfUS = appleProducts1.selectExpr("Model_name","PriceInDollars as PriceUS").where(col("Country")=="United States")

    differencePriceCountry = appleProducts1.join(dfUS,appleProducts1.Model_name==dfUS.Model_name,how='inner')

    differencePriceCountry = differencePriceCountry.groupBy("Country").agg(avg("PriceUS").alias("moyenneUS"),avg("PriceInDollars").alias("moyennePrice"))

    differencePriceCountry = differencePriceCountry.withColumn("écart",(((col("moyennePrice")/col("moyenneUS")))-1)* 100).orderBy(col("moyennePrice").desc())
    differencePriceCountry.write.mode("append").format("csv").option("sep",";").save("./")


################## EXERCICE 3 ###############################
    # Réaliser un DF comprenant le pays et le coût total afin d'acheter tout les produits Apple disponible au sein du pays. 
    # Enregistrer ce DF en csv (coutTotal.csv)
    costProducts = appleProducts1.groupBy("Country").sum("PriceInDollars")
    costProducts = costProducts.select("Country","sum(PriceInDollars)")
    costProducts.write.mode("append").format("csv").option("sep",";").save("./")    




################## EXERCICE 4 ###############################
    # Réaliser un DF avec le nom des produits disponibles
    # Enregistrer ce DF en csv (listeProduit.csv)

    # 1e version - Afficher tout les produist (avec duplicata)
    # appleProducts.selectExpr("Model_name as Produit").write.mode("append").format("csv").option("sep",";").save("./")
    
    # 2e version - Afficher tout les produits (sans duplicat - en considérant que les United States possède tout les produits)
    allProductsShow = appleProducts.where(col("Country").contains("United States"))
    allProductsShow = allProductsShow.select("Model_name")
    allProductsShow.write.mode("append").format("csv").option("sep",";").save("./")





################## EXERCICE 5 ###############################
    # Realiser un DF afin de trouver les pays proposant les AirPodsPro au prix le plus abordable 
    # Enregistrer ce DF en csv (airpodsPro.csv)
    appleProducts2 = appleProducts.join(conversion, appleProducts.Currency == conversion.ISO_4217).withColumn("PriceInDollars", col("Price")/col("Dollar_To_Curr_Ratio"))
    appleProducts2 = appleProducts2.select("Model_name","Price","Country","Currency","PriceInDollars")
    appleProducts2.select("*").where(col("Model_name").contains("AirPods Pro")).sort("PriceInDollars").write.mode("append").format("csv").option("sep",";").save("./")






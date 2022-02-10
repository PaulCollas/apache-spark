# Efrei - Apache Spark - Apple Dataset


# 📚 Dependecies & sources 

Here is the list of all the dependencies and their sources :

- [SparkSQL](https://spark.apache.org/downloads.html)
- [Python](https://www.python.org/downloads/)

...

# 📌 Commands to launch project

Start to clone the project
```
git clone https://github.com/PaulCollas/apache-spark.git
```

On terminal (in the same repo):
```
spark-submit --master local COLLAS_PAUL_WEBDMFS1.py ApplePrices.csv CurrencyConversion.csv
```

If u want to launch commands from python script :
```
pyspark
```

# 🏆 Goal

Work on a Dataset of data on the sales prices of Apple products in
different countries.
<br>
<br>
The data was recovered using a scrapping tool on Apple sites in different countries.
I assume that the price of products sold in the United States is the true price of products.
The project is to **create a Python script** to be rendered which will have to respond to the different
requests.

# 🚀 Launch commands

The python script is divised by many commands :

## To load csv file
```
appleProducts = spark.read.format("csv").option("header", "true").option("header", "true").option("inferSchema", "true").load(sys.argv[1])
```
```
conversion= spark.read.format("csv").option("header", "true").option("header", "true").option("inferSchema", "true").load(sys.argv[2])
```

## Q1
To convert the prices of the different products into dollars
```
appleProducts1 = appleProducts.join(conversion, appleProducts.Currency == conversion.ISO_4217).withColumn("PriceInDollars", col("Price")/col("Dollar_To_Curr_Ratio"))
```
```
appleProducts1 = appleProducts1.select("Model_name","Price","Country","Currency","PriceInDollars")
```

## Q2


Establish the average price of the products of each country and compare to the equivalent of the classic US price.
<br>
<br>
Addition of a column of the percentage of difference between the average of the products in the US and the average of the products sold in the country.
<br>
<br>
Sort the DF in ascending order
<br>
<br>
Save this DF as csv

<br>

```
dfUS = appleProducts1.selectExpr("Model_name","PriceInDollars as PriceUS").where(col("Country")=="United States")
```
```
differencePriceCountry = appleProducts1.join(dfUS,appleProducts1.Model_name==dfUS.Model_name,how='inner')
```
```
differencePriceCountry = differencePriceCountry.groupBy("Country").agg(avg("PriceUS").alias("moyenneUS"),avg("PriceInDollars").alias("moyennePrice"))
```
```
differencePriceCountry = differencePriceCountry.withColumn("écart",(((col("moyennePrice")/col("moyenneUS")))-1)* 100).orderBy(col("moyennePrice").desc())
```
```
differencePriceCountry.write.mode("append").format("csv").option("sep",";").save("./")
```

## Q3

Make a DF including the country and the total cost in order to buy all the Apple products available within the country.

```
costProducts = appleProducts1.groupBy("Country").sum("PriceInDollars")
```
```
costProducts = costProducts.select("Country","sum(PriceInDollars)")
```
```
costProducts.write.mode("append").format("csv").option("sep",";").save("./") 
```

## Q4

Make a DF with the name of the available products

```
allProductsShow = appleProducts.where(col("Country").contains("United States"))
```
```
allProductsShow = allProductsShow.select("Model_name")
```
```
allProductsShow.write.mode("append").format("csv").option("sep",";").save("./")
```

## Q5

Make a DF to find the countries offering the AirPodsPro at the most affordable price

```
appleProducts2 = appleProducts.join(conversion, appleProducts.Currency == conversion.ISO_4217).withColumn("PriceInDollars", col("Price")/col("Dollar_To_Curr_Ratio"))
```
```
appleProducts2 = appleProducts2.select("Model_name","Price","Country","Currency","PriceInDollars")
```
```
appleProducts2.select("*").where(col("Model_name").contains("AirPods Pro")).sort("PriceInDollars").write.mode("append").format("csv").option("sep",";").save("./")
```

# 💻 DataVisualisation 

## Q1

After launch commands for load csv u can run this commands :

```
appleProducts1.show()
```

You should have a table similar to this :


|          Model_name|  Price|  Country|Currency|    PriceInDollars|
|        ---|---|---|     ---|--- |
|        24-inch iMac|1919.01|Australia|     AUD|1370.7214285714288|
|         AirPods Max| 908.47|Australia|     AUD|  648.907142857143|
|         AirPods Pro|  403.2|Australia|     AUD|             288.0|
|AirPods(2nd gener...| 221.31|Australia|     AUD|158.07857142857145|
|AirPods(3rd gener...| 281.94|Australia|     AUD| 201.3857142857143|
|Apple Pencil (2nd...|  201.1|Australia|     AUD|143.64285714285714|
|         Apple TV 4K| 251.62|Australia|     AUD|179.72857142857146|
|      Apple Watch SE| 433.52|Australia|     AUD|309.65714285714284|
|Apple Watch Series 3| 302.15|Australia|     AUD|215.82142857142856|
|         MacBook Air| 1514.8|Australia|     AUD|            1082.0|
|         Magic Mouse| 110.15|Australia|     AUD| 78.67857142857143|
|          Sport Band|  69.73|Australia|     AUD|49.807142857142864|
|                iPad| 504.26|Australia|     AUD| 360.1857142857143|
|            iPad Pro|1211.64|Australia|     AUD|  865.457142857143|
|           iPhone 12|1009.53|Australia|     AUD| 721.0928571428572|
|           iPhone 13|1211.64|Australia|     AUD|  865.457142857143|
|           iPhone SE| 686.16|Australia|     AUD| 490.1142857142857|
|        24-inch iMac|1597.97|   Canada|     CAD| 1258.244094488189|
|         AirPods Max|  778.5|   Canada|     CAD|  612.992125984252|
|         AirPods Pro| 328.79|   Canada|     CAD| 258.8897637795276|


## Q2

After launch commands for Q2 u can run this commands :

```
differencePriceCountry.show()
```

You should have a table similar to this :

|       Country|        moyenneUS|      moyennePrice|             écart|
|       ---|          ---| ---| ---|
|        Sweden|          412.125| 551.4025273224042| 33.79497174944597|
|        Norway|          412.125| 548.2195224719101|  33.0226320829627|
|       Denmark|          412.125| 547.2875569044006| 32.79649545754337|
|         India|420.1764705882353| 546.9993360418609|30.183238313196647|
|Czech Republic|          412.125|  546.025547235023| 32.49027533758522|
|       Ireland|420.1764705882353| 545.4738929279576| 29.82019011305166|
|       Finland|          412.125| 538.7176966292134|  30.7170631796696|
|      Portugal|          412.125| 538.7176966292134|  30.7170631796696|
|        France|420.1764705882353| 537.5049570389955|27.923621302854862|
|       Austria|420.1764705882353| 533.5214805023134| 26.97557284809362|
|       Germany|420.1764705882353| 532.4395241242564|26.718072380125424|
|       Hungary|          412.125| 530.8413199341796|28.805901106261356|
|   Netherlands|          412.125| 526.9494382022472|27.861556130360256|
|         Italy|           402.75| 523.9016853932584| 30.08111369168427|
|        Poland|          412.125| 523.3557692307693|26.989570938615536|
|         Spain|           402.75|511.90800561797755|27.103167130472382|
|    Luxembourg|          412.125|          509.3125|23.582044282681224|
|        Mexico|           402.75| 507.6251219512195| 26.03975715734812|
|        Russia|            393.0|504.74545533997554|28.433958101774937|
|   Philippines|          412.125| 477.6060498828583|15.888638127475474|


## Q3

After launch commands for Q3 u can run this commands :

```
costProducts.show()
```

You should have a table similar to this :

|       Country|sum(PriceInDollars)|
|        ---|  ---|
|        Russia|  9082.051258521236|
|        Sweden|   9008.66775956284|
|   Philippines|  7816.797149550957|
|       Germany|   9051.47191011236|
|        France|  9137.584269662922|
|       Finland|   8798.96629213483|
|         India|  9512.188255845202|
| United States|             7143.0|
|         Italy|  9724.561797752809|
|        Norway|  8962.351685393258|
|         Spain|   9498.79775280899|
|       Denmark|  8953.717754172989|
|       Ireland|   9273.05617977528|
|      Thailand| 7096.7852249031885|
|        Mexico|  9390.375609756098|
|        Canada|  7344.070866141731|
|Czech Republic|  8927.820276497696|
|    Luxembourg|  8322.539325842696|
|        Poland|  8558.955334987593|
|      Portugal|   8798.96629213483|


## Q4

After launch commands for Q4 u can run this commands :

```
allProductsShow.show()
```

You should have a table similar to this :

|          Model_name|
|        ---|
|        24-inch iMac|
|         AirPods Max|
|         AirPods Pro|
|AirPods(2nd gener...|
|AirPods(3rd gener...|
|Apple Pencil (2nd...|
|         Apple TV 4K|
|      Apple Watch SE|
|Apple Watch Series 3|
|         MacBook Air|
|         Magic Mouse|
|          Sport Band|
|                iPad|
|            iPad Pro|
|           iPhone 12|
|           iPhone 13|
|           iPhone SE|

## Q5

After launch commands for Q5 u can run this commands :

```
appleProducts2.show()
```

You should have a table similar to this :

|          Model_name|  Price|  Country|Currency|    PriceInDollars|
|          ---|  ---|  ---|---|    ---|
|        24-inch iMac|1919.01|Australia|     AUD|1370.7214285714288|
|         AirPods Max| 908.47|Australia|     AUD|  648.907142857143|
|         AirPods Pro|  403.2|Australia|     AUD|             288.0|
|AirPods(2nd gener...| 221.31|Australia|     AUD|158.07857142857145|
|AirPods(3rd gener...| 281.94|Australia|     AUD| 201.3857142857143|
|Apple Pencil (2nd...|  201.1|Australia|     AUD|143.64285714285714|
|         Apple TV 4K| 251.62|Australia|     AUD|179.72857142857146|
|      Apple Watch SE| 433.52|Australia|     AUD|309.65714285714284|
|Apple Watch Series 3| 302.15|Australia|     AUD|215.82142857142856|
|         MacBook Air| 1514.8|Australia|     AUD|            1082.0|
|         Magic Mouse| 110.15|Australia|     AUD| 78.67857142857143|
|          Sport Band|  69.73|Australia|     AUD|49.807142857142864|
|                iPad| 504.26|Australia|     AUD| 360.1857142857143|
|            iPad Pro|1211.64|Australia|     AUD|  865.457142857143|
|           iPhone 12|1009.53|Australia|     AUD| 721.0928571428572|
|           iPhone 13|1211.64|Australia|     AUD|  865.457142857143|
|           iPhone SE| 686.16|Australia|     AUD| 490.1142857142857|
|        24-inch iMac|1597.97|   Canada|     CAD| 1258.244094488189|
|         AirPods Max|  778.5|   Canada|     CAD|  612.992125984252|
|         AirPods Pro| 328.79|   Canada|     CAD| 258.8897637795276|


# 💬 Export to CSV

Normally after the launch of the script you must have 4 csv which must be generated



# 🐵 Credits
Craft with ❤️ in Paris.

Created by **Paul Collas**




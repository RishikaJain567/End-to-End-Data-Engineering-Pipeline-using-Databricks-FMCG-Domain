# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Consolidated_Pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, gold_schema, silver_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg", "Catalog")
dbutils.widgets.text("datasource","customers","Datasource")

# COMMAND ----------

# DBTITLE 1,read widget values
catalog = dbutils.widgets.get("catalog")
datasource = dbutils.widgets.get("datasource")

base_path = f's3://sportsbar-rd/{datasource}/*.csv'
print(base_path)


# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp",F.current_timestamp())
        .select("*","_metadata.file_name","_metadata.file_size")
)

display(df.limit(10))



# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.{datasource}")    

# format("delta") means Stores the data as a Delta Lake table.
# option("delta.enableChangeDataFeed", "true") means Enable Change Data Feed(CDF) for the table, whenevrer the table is updated,deleted, inserted  - delta records those changes
# mode("overwrite")-Removes existing data and writes the new data.

# COMMAND ----------

# MAGIC %md 
# MAGIC Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{datasource};")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,finding the duplicates
df_duplicates = df_bronze.groupBy("customer_id").count().where("count > 1")
display(df_duplicates)


# COMMAND ----------

# DBTITLE 1,dropping the duplicates
print('Rows before duplicates dropped:', df_bronze.count())
df_silver = df_bronze.dropDuplicates(["customer_id"])
print('Rows after duplicates dropped:', df_silver.count())


# COMMAND ----------

# DBTITLE 1,display the name with leading spaces
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("Customer_name")))
)

# COMMAND ----------

# DBTITLE 1,actually trimming
df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))

)
#F.col() tells PySpark that customer_name is a column in the DataFrame, not a normal Python string.

# COMMAND ----------

# DBTITLE 1,running again
#running again to check still any value present with spaces or not
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("Customer_name")))
)

# COMMAND ----------

# DBTITLE 1,finding issue with city names
df_silver.select('city').distinct().show()

# COMMAND ----------

# DBTITLE 1,typos-city mapping
city_mapping = {
    'Bengaluruu' : 'Bengaluru',
    'Bengalore' : 'Bengaluru',

    'Hyderabadd' : 'Hyderabad',
    'Hyderbad' : 'Hyderabad',

    'NewDelhee' : 'New Delhi',
    'NewDelhi' : 'New Delhi',
    'NewDheli' : 'New Delhi'
    }

allowed = ["Bengaluru", "Hyderabad", "New Delhi"] 

df_silver = (
    df_silver 
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(),None)
        .when(F.col("city").isin(allowed), F.col("city"))
        .otherwise(None)
    )
)

#"First, we standardize city names using .replace(), for example converting 'Bangalore' to 'Bengaluru'. Then we use withColumn() with when() conditions to validate the city. If the city is already NULL, we leave it as NULL. If the city is one of the approved values using isin(), we keep it. Otherwise, we replace it with NULL.

df_silver.select('city').distinct().show()

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# DBTITLE 1,fixing of customer name
df_silver = df_silver.withColumn(
    "customer_name", 
    F.when(F.col("customer_name").isNull(), None)
     .otherwise(F.initcap("customer_name"))
)

# initcap function - is used to capitalize the first letter of each word in a string and convert the remaining letters to lowercase.

df_silver.select('customer_name').show()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

#truncate = False is used to Don't shorten long strings.

# COMMAND ----------

null_customer_name = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_name)).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,filling null cities with some names
#business confirmation note : city correlation confirmed by business team

customer_city_fix = {
    #Sprintx nutrition
    789403 : "New Delhi",
    
    #Zenathlete Foods
    789420 : "Bengaluru",

    #Primefuel Nutrition
    789521 : "Hyderabad",

    #Recovery Lane
    789603 : "Hyderabad"
    
}

df_fix = spark.createDataFrame(
    [(k,v) for k,v in customer_city_fix.items() ],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

# DBTITLE 1,joining this fixing city df to original one
df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city") #replace null with fixed_city   
    )
    .drop("fixed_city")
)

#This joins df_silver with df_fix using the common column customer_id.
#Coalesce - It checks the values from left to right and returns the first non-null value. Is city NULL?
    #No  → Use city
    #Yes → Use fixed_city

display(df_silver)


# COMMAND ----------

# DBTITLE 1,change data type of customer_id
# we have to change the datatype of customer_id as it is in string in our gold layer
df_silver = df_silver.withColumn("customer_id",F.col("customer_id").cast("string"))

print(df_silver.printSchema())

# COMMAND ----------

# DBTITLE 1,edit the column names acc to the gold layer of parent company
#making the column name same as the gold layer of parent company
df_silver = (
    df_silver
    #build final customer column : "customerName-city" or "CustomerName-unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )

    #static attributes aligned with parent model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
    
)

#coalesce() returns the first non-null value.
#It means:
#If city has a value → use it.
#If city is NULL → use "Unknown".

display(df_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,writing it to silver layer
df_silver.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{datasource}")    

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"Select * from {catalog}.{silver_schema}.{datasource};")

#take required cols only
df_gold =df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel") 

# COMMAND ----------

# DBTITLE 1,writ it in Gold layer
df_gold.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{datasource}") 

    #here we can't use dim_customer as it is the gold layer name of parent company
    #so, that's why we have used sb_dim_{datasource}

# COMMAND ----------

# DBTITLE 1,merging
delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customer = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

#If you only need to read or transform data, use spark.table().
#If you need to modify an existing Delta table (update, delete, or upsert records), first load it with DeltaTable.forName().

# COMMAND ----------

# DBTITLE 1,upsert both gold layers
delta_table.alias("target").merge(
    source = df_child_customer.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
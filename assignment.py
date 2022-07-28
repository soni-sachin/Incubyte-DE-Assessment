from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.window import Window
import pyspark.sql.functions as f

# Creating spark Session
spark = SparkSession.builder.getOrCreate()
ind_data = spark.read.csv(
    "/Users/sachinsoni/Downloads/Incubyte DE Assessment/SampleInputData/IND.csv",
    header="true",
    inferSchema="true",
)
us_data = spark.read.csv(
    "/Users/sachinsoni/Downloads/Incubyte DE Assessment/SampleInputData/USA.csv",
    header="true",
    inferSchema="true",
)
aus_data = pd.read_excel(
    "/Users/sachinsoni/Downloads/Incubyte DE Assessment/SampleInputData/AUS.xlsx",
    engine="openpyxl",
)


# Filteration of Data eg - Datatime column issue
us_data = us_data.withColumn(
    "VaccinationDate", us_data["VaccinationDate"].cast(StringType())
)
us_data = us_data.withColumn(
    "VaccinationDate",
    when(
        length("VaccinationDate") < 8, format_string("0%s", "VaccinationDate")
    ).otherwise(format_string("%s", "VaccinationDate")),
)
us_data = us_data.withColumn("VaccinationDate", to_date("VaccinationDate", "MMddyyyy"))
aus_data["Date of Vaccination"] = pd.to_datetime(
    aus_data["Date of Vaccination"], errors="coerce"
)
aus_data = spark.createDataFrame(aus_data)

# Rename column Name
ind_data = (
    ind_data.withColumnRenamed("ID", "ID")
    .withColumnRenamed("Name", "NAME")
    .withColumnRenamed("DOB", "DOB")
    .withColumnRenamed("VaccinationType", "VACCINATIONTYPE")
    .withColumnRenamed("VaccinationDate", "VACCINATIONTDATE")
    .withColumnRenamed("Free or Paid", "COST")
)

us_data = (
    us_data.withColumnRenamed("ID", "ID")
    .withColumnRenamed("Name", "NAME")
    .withColumnRenamed("VaccinationType", "VACCINATIONTYPE")
    .withColumnRenamed("VaccinationDate", "VACCINATIONTDATE")
)

aus_data = (
    aus_data.withColumnRenamed("Unique ID", "ID")
    .withColumnRenamed("Patient Name", "NAME")
    .withColumnRenamed("Vaccine Type", "VACCINATIONTYPE")
    .withColumnRenamed("Date of Birth", "DOB")
    .withColumnRenamed("Date of Vaccination", "VACCINATIONTDATE")
)

# Filteration of Data
for column in [column for column in us_data.columns if column not in ind_data.columns]:
    ind_data = ind_data.withColumn(column, lit(None))

for column in [column for column in ind_data.columns if column not in us_data.columns]:
    us_data = us_data.withColumn(column, lit(None))

for column in [column for column in ind_data.columns if column not in aus_data.columns]:
    aus_data = aus_data.withColumn(column, lit(None))


us_data = us_data.select(ind_data.columns)
aus_data = aus_data.select(ind_data.columns)
ind_data = ind_data.withColumn("COUNTRY", lit("INDIA")).withColumn(
    "TOTAL_POPULATION", lit("250")
)
us_data = us_data.withColumn("COUNTRY", lit("US")).withColumn(
    "TOTAL_POPULATION", lit("150")
)
aus_data = aus_data.withColumn("COUNTRY", lit("AUS")).withColumn(
    "TOTAL_POPULATION", lit("110")
)
final_Df = [ind_data, us_data, aus_data]

# Creating Single source Data
FINAL_DF = reduce(DataFrame.unionAll, final_Df)
FINAL_DF = FINAL_DF.withColumn(
    "ID", f.row_number().over(Window.partitionBy().orderBy("NAME"))
)
# First Solution
Metric1 = (
    FINAL_DF.select("COUNTRY", "VACCINATIONTYPE")
    .groupBy("COUNTRY", "VACCINATIONTYPE")
    .count()
)

# Second Solution
Matric2 = FINAL_DF.groupBy("COUNTRY").agg(
    (f.count("ID") / (f.min("TOTAL_POPULATION")) * 100).alias("% VACCINATED")
)

# Third Solution
# Calculating no of vaccination
dummy = FINAL_DF.withColumn("COUNT", lit(FINAL_DF.count()))
Matric3 = (
    dummy.groupBy("COUNTRY", "COUNT")
    .agg((f.count("NAME") / (f.col("COUNT")) * 100).alias("% CONTRIBUTION"))
    .drop("COUNT")
)


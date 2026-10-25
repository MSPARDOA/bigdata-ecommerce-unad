from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count

spark = SparkSession.builder \
    .appName("ECommerceBatchAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

data = [
    (101, "Electronica", 450.0, 2),
    (102, "Ropa", 50.0, 3),
    (103, "Hogar", 120.0, 1),
    (104, "Electronica", 300.0, 1),
    (105, "Deportes", 80.0, 4),
    (106, "Ropa", 45.0, 2),
    (107, "Hogar", 210.0, 2),
    (108, "Electronica", 600.0, 1)
]
columns = ["product_id", "category", "price", "quantity"]
df = spark.createDataFrame(data, columns)

print("=== 1. DATOS ORIGINALES DE VENTAS (HEAD) ===")
df.show()

df_transformed = df.withColumn("total_revenue", col("price") * col("quantity"))

print("=== 2. DATOS TRANSFORMADOS CON INGRESO TOTAL ===")
df_transformed.show()

print("=== 3. ANÁLISIS EXPLORATORIO: Resumen de Ventas por Categoría ===")
eda_result = df_transformed.groupBy("category") \
    .agg(
        count("product_id").alias("total_transacciones"),
        sum("total_revenue").alias("ingresos_totales"),
        avg("price").alias("precio_promedio")
    )

eda_result.show()
spark.stop()

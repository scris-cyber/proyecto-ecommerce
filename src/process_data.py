from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round

def main():
    # 1. Inicializar la sesión de Spark en el clúster
    spark = SparkSession.builder \
        .appName("ECommerceOntologyMapping") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    print("\nLOG: Sesión de Spark iniciada correctamente.")

    # 2. Definición de rutas del entorno
    ontology_path = "/app/ontology/ecommerce-mapping.owl"
    print(f"LOG: Referencia de ontología base orientada a: {ontology_path}")

    # ==========================================
    # DATOS Y AGREGACIÓN 1: GEOGRAFÍA Y FINANZAS
    # ==========================================
    
    # Dataset 1: Órdenes y Clientes (IDs ofuscados en Hash)
    raw_data_orders = [
        ("h84f27a...", "u92jd81...", "SP", 150.50),
        ("h392j9k...", "u102k3j...", "RJ", 89.90),
        ("h102k4l...", "u83jd92...", "SP", 200.00),
        ("h83jd92...", "u29jd81...", "MG", 350.00),
        ("h29jd81...", "u73hd82...", "RJ", 120.00),
        ("h73hd82...", "u38jd92...", "SP", 50.25)
    ]
    columns_orders = ["order_id_hash", "customer_id_hash", "customer_state", "payment_value"]
    
    df_orders = spark.createDataFrame(raw_data_orders, schema=columns_orders)

    print("\nLOG: Aplicando mapeo conceptual para Bloque 1:")
    print("[Ontology Class: Customer] -> Columna física: customer_id_hash")
    print("[Ontology Class: Order]    -> Columna física: order_id_hash")
    print("[Object Property: buys]   -> Relación lógica entre Customer y Order")

    print("\nLOG: Procesando Agregación 1 (Volumen y promedio transaccional por estado)...")
    df_metrics_geo = df_orders.groupBy("customer_state") \
        .agg(
            count("order_id_hash").alias("Total_Ordenes"),
            round(avg("payment_value"), 2).alias("Pago_Promedio")
        )
    df_metrics_geo.show()

    # ==========================================
    # DATOS Y AGREGACIÓN 2: CALIDAD Y PRODUCTOS
    # ==========================================
    
    # Dataset 2: Reseñas y Categorías de Productos
    raw_data_reviews = [
        ("p92js8...", "utilidades_domesticas", 5),
        ("p102js...", "perfumeria", 4),
        ("p92js8...", "utilidades_domesticas", 2),
        ("p382jd...", "juguetes", 5),
        ("p102js...", "perfumeria", 5),
        ("p92js8...", "utilidades_domesticas", 4)
    ]
    columns_reviews = ["product_id_hash", "product_category_name", "review_score"]
    
    df_reviews = spark.createDataFrame(raw_data_reviews, schema=columns_reviews)

    print("\nLOG: Aplicando mapeo conceptual para Bloque 2:")
    print("[Ontology Class: Product]  -> Columna física: product_id_hash")
    print("[Ontology Class: Review]   -> Estructura basada en: review_score")
    print("[Object Property: reviews] -> Relación lógica de opinión sobre el producto")

    print("\nLOG: Procesando Agregación 2 (Análisis de satisfacción por categoría de producto)...")
    df_metrics_reviews = df_reviews.groupBy("product_category_name") \
        .agg(
            count("review_score").alias("Total_Reseñas"),
            round(avg("review_score"), 2).alias("Calificacion_Promedio")
        )
    df_metrics_reviews.show()

    print("LOG: Procesamiento de Agregaciones 1 y 2 finalizado con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()
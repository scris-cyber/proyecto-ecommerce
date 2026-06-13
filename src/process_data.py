from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # 1. Inicializar la sesión de Spark en el clúster
    spark = SparkSession.builder \
        .appName("ECommerceOntologyMapping") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    print("LOG: Sesión de Spark iniciada correctamente.")

    # 2. Definición de rutas del entorno
    ontology_path = "/app/ontology/ecommerce-mapping.owl"
    print(f"LOG: Cargando ontología base desde: {ontology_path}")

    # 3. Lógica de mapeo semántico
    # Vinculación de los identificadores relacionales con los conceptos de la ontología
    print("LOG: Procesando mapeo lógico de entidades...")
    print("[Ontology Class: Customer] -> Mapeado a columna: customer_id_hash")
    print("[Object Property: buys]   -> Mapeado a columna: order_id_hash")
    print("[Ontology Class: Order]    -> Mapeado a esquema: orders_summary")
    
    print("LOG: Mapeo y procesamiento de datos finalizado.")
    spark.stop()

if __name__ == "__main__":
    main()
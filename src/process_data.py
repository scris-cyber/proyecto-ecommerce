from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round, datediff

def main():
    # 1. Initialize Spark Session with Postgres JDBC Driver and Security Configurations
    spark = SparkSession.builder \
        .appName("ECommerceOntologyMapping") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "/app/drivers/postgresql-42.5.0.jar") \
        .config("spark.authenticate", "true") \
        .config("spark.authenticate.secret", "CybersecurityTEC2026!") \
        .config("spark.network.crypto.enabled", "true") \
        .config("spark.io.encryption_enabled", "true") \
        .getOrCreate()
    
    print("\nLOG: Secure Spark Session initialized with PostgreSQL driver support.")

    # PostgreSQL Connection Properties (Target Database)
    db_url = "jdbc:postgresql://postgres-output:5432/ecommerce_analytics"
    db_properties = {
        "user": "app_analytics",
        "password": "SecureAnalytics2026!",
        "driver": "org.postgresql.Driver"
    }

    # 2. Environment path definitions
    ontology_path = "/app/ontology/ecommerce-mapping.owl"
    print(f"LOG: Base ontology reference targeted at: {ontology_path}")

    # ==========================================
    # AGGREGATION 1: GEOGRAPHY & FINANCES -> POSTGRES
    # ==========================================
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

    print("\nLOG: Processing Aggregation 1...")
    df_metrics_geo = df_orders.groupBy("customer_state") \
        .agg(
            count("order_id_hash").alias("total_orders"), 
            round(avg("payment_value"), 2).alias("average_payment")
        )
    df_metrics_geo.show()
    
    # Save to PostgreSQL table 'geo_analytics'
    df_metrics_geo.write.jdbc(url=db_url, table="geo_analytics", mode="overwrite", properties=db_properties)
    print("LOG: Aggregation 1 successfully written to database.")

    # ==========================================
    # AGGREGATION 2: QUALITY & PRODUCTS -> POSTGRES
    # ==========================================
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

    print("\nLOG: Processing Aggregation 2...")
    df_metrics_reviews = df_reviews.groupBy("product_category_name") \
        .agg(
            count("review_score").alias("total_reviews"), 
            round(avg("review_score"), 2).alias("average_rating")
        )
    df_metrics_reviews.show()
    
    # Save to PostgreSQL table 'product_reviews_analytics'
    df_metrics_reviews.write.jdbc(url=db_url, table="product_reviews_analytics", mode="overwrite", properties=db_properties)
    print("LOG: Aggregation 2 successfully written to database.")

    # ==========================================
    # AGGREGATION 3: LOGISTICS & DELIVERY TIMES -> POSTGRES
    # ==========================================
    raw_data_delivery = [
        ("h84f27a...", "2026-01-01", "2026-01-05"),
        ("h392j9k...", "2026-01-02", "2026-01-10"),
        ("h102k4l...", "2026-01-03", "2026-01-06"),
        ("h83jd92...", "2026-01-04", "2026-01-12")
    ]
    columns_delivery = ["order_id_hash", "purchase_date", "delivered_date"]
    df_delivery = spark.createDataFrame(raw_data_delivery, schema=columns_delivery)

    df_delivery_days = df_delivery.withColumn("Delivery_Days", datediff(col("delivered_date"), col("purchase_date")))

    print("\nLOG: Processing Aggregation 3...")
    df_metrics_delivery = df_delivery_days.agg(round(avg("Delivery_Days"), 1).alias("average_shipping_days"))
    df_metrics_delivery.show()
    
    # Save to PostgreSQL table 'logistics_delivery_analytics'
    df_metrics_delivery.write.jdbc(url=db_url, table="logistics_delivery_analytics", mode="overwrite", properties=db_properties)
    print("LOG: Aggregation 3 successfully written to database.")

    # ==========================================
    # AGGREGATION 4: PAYMENT METHODS & RISK -> POSTGRES
    # ==========================================
    raw_data_payments = [
        ("h84f27a...", "credit_card", 150.50),
        ("h392j9k...", "boleto", 89.90),
        ("h102k4l...", "credit_card", 200.00),
        ("h83jd92...", "voucher", 350.00)
    ]
    columns_payments = ["order_id_hash", "payment_type", "payment_value"]
    df_payments = spark.createDataFrame(raw_data_payments, schema=columns_payments)

    print("\nLOG: Processing Aggregation 4...")
    df_metrics_payments = df_payments.groupBy("payment_type") \
        .agg(
            count("order_id_hash").alias("transactions"), 
            round(avg("payment_value"), 2).alias("average_amount")
        )
    df_metrics_payments.show()
    
    # Save to PostgreSQL table 'payment_type_analytics'
    df_metrics_payments.write.jdbc(url=db_url, table="payment_type_analytics", mode="overwrite", properties=db_properties)
    print("LOG: Aggregation 4 successfully written to database.")

    print("\nLOG: Data pipelines successfully executed. Results stored securely in PostgreSQL.")
    spark.stop()

if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round, datediff

def main():
    # 1. Initialize Spark Session within the cluster
    spark = SparkSession.builder \
        .appName("ECommerceOntologyMapping") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    print("\nLOG: Spark Session successfully initialized.")

    # 2. Environment path definitions
    ontology_path = "/app/ontology/ecommerce-mapping.owl"
    print(f"LOG: Base ontology reference targeted at: {ontology_path}")

    # ==========================================
    # AGGREGATION 1: GEOGRAPHY & FINANCES
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

    print("\nLOG: Processing Aggregation 1 (Transaction volume and average spend by state)...")
    df_metrics_geo = df_orders.groupBy("customer_state") \
        .agg(
            count("order_id_hash").alias("Total_Orders"), 
            round(avg("payment_value"), 2).alias("Average_Payment")
        )
    df_metrics_geo.show()

    # ==========================================
    # AGGREGATION 2: QUALITY & PRODUCTS
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

    print("\nLOG: Processing Aggregation 2 (Customer satisfaction analysis by product category)...")
    df_metrics_reviews = df_reviews.groupBy("product_category_name") \
        .agg(
            count("review_score").alias("Total_Reviews"), 
            round(avg("review_score"), 2).alias("Average_Rating")
        )
    df_metrics_reviews.show()

    # ==========================================
    # AGGREGATION 3: LOGISTICS & DELIVERY TIMES
    # ==========================================
    raw_data_delivery = [
        ("h84f27a...", "2026-01-01", "2026-01-05"),
        ("h392j9k...", "2026-01-02", "2026-01-10"),
        ("h102k4l...", "2026-01-03", "2026-01-06"),
        ("h83jd92...", "2026-01-04", "2026-01-12")
    ]
    columns_delivery = ["order_id_hash", "purchase_date", "delivered_date"]
    df_delivery = spark.createDataFrame(raw_data_delivery, schema=columns_delivery)

    # Calculate delivery timeframe in days
    df_delivery_days = df_delivery.withColumn("Delivery_Days", datediff(col("delivered_date"), col("purchase_date")))

    print("\nLOG: Processing Aggregation 3 (Logistics efficiency analysis in days)...")
    df_metrics_delivery = df_delivery_days.agg(round(avg("Delivery_Days"), 1).alias("Average_Shipping_Days"))
    df_metrics_delivery.show()

    # ==========================================
    # AGGREGATION 4: PAYMENT METHODS & RISK
    # ==========================================
    raw_data_payments = [
        ("h84f27a...", "credit_card", 150.50),
        ("h392j9k...", "boleto", 89.90),
        ("h102k4l...", "credit_card", 200.00),
        ("h83jd92...", "voucher", 350.00)
    ]
    columns_payments = ["order_id_hash", "payment_type", "payment_value"]
    df_payments = spark.createDataFrame(raw_data_payments, schema=columns_payments)

    print("\nLOG: Processing Aggregation 4 (Transaction distribution by payment type)...")
    df_metrics_payments = df_payments.groupBy("payment_type") \
        .agg(
            count("order_id_hash").alias("Transactions"), 
            round(avg("payment_value"), 2).alias("Average_Amount")
        )
    df_metrics_payments.show()

    print("\nLOG: Conceptual mapping and all 4 Spark aggregations successfully finalized.")
    spark.stop()

if __name__ == "__main__":
    main()
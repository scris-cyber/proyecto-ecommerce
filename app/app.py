import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Configuración de la Base de Datos PostgreSQL
# Cambia 'app_analytics' y 'SecureAnalytics2026!' por tus credenciales de la rúbrica
DB_URL = "postgresql://app_analytics:SecureAnalytics2026!@localhost:5432/ecommerce_analytics"
@st.cache_resource
def init_connection():
    return create_engine(DB_URL)

def load_data(table_name):
    try:
        engine = init_connection()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=engine)
        return df
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return pd.DataFrame()

# Configuración de la página
st.set_page_config(page_title="E-Commerce Analytics Dashboard", page_icon="📊")

def main():
    # Manejo de sesión para Login
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if not st.session_state['logged_in']:
        st.title("🔐 Secure Analytics Dashboard - E-Commerce")
        st.subheader("Please log in to access the analytical reports")

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Log In"):
            if username == "admin" and password == "SecureDashboard2026!":
                st.session_state['logged_in'] = True
                st.success("Authentication successful! Redirecting...")
                st.rerun()
            else:
                st.error("Invalid username or password. Access Denied.")
    
    else:
        # Una vez logueado, mostrar el dashboard
        st.sidebar.success("Logged in as Administrator")
        if st.sidebar.button("Log Out"):
            st.session_state['logged_in'] = False
            st.rerun()

        st.title("📊 E-Commerce Analytics Reports")
        st.write("Below are the aggregations processed by Spark and stored in PostgreSQL.")

        # 1. Geography & Finances
        st.subheader("🌍 1. Geographic & Financial Metrics (geo_analytics)")
        df_geo = load_data("geo_analytics")
        if not df_geo.empty:
            st.dataframe(df_geo, use_container_width=True)
        else:
            st.warning("No data found in geo_analytics table.")

        # 2. Quality & Products
        st.subheader("⭐ 2. Product Reviews & Ratings (product_reviews_analytics)")
        df_reviews = load_data("product_reviews_analytics")
        if not df_reviews.empty:
            st.dataframe(df_reviews, use_container_width=True)
        else:
            st.warning("No data found in product_reviews_analytics table.")

        # 3. Logistics
        st.subheader("🚚 3. Logistics & Shipping Times (logistics_delivery_analytics)")
        df_delivery = load_data("logistics_delivery_analytics")
        if not df_delivery.empty:
            st.dataframe(df_delivery, use_container_width=True)
        else:
            st.warning("No data found in logistics_delivery_analytics table.")

        # 4. Payment Methods
        st.subheader("💳 4. Payment Methods Risk Analysis (payment_type_analytics)")
        df_payments = load_data("payment_type_analytics")
        if not df_payments.empty:
            st.dataframe(df_payments, use_container_width=True)
        else:
            st.warning("No data found in payment_type_analytics table.")

if __name__ == '__main__':
    main()
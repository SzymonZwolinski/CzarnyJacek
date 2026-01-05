import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
import psycopg2
import pandas as pd
import time
import bcrypt
import os

# Konfiguracja
DB_HOST = "db"
DB_NAME = "casino_db"
DB_USER = "admin"
DB_PASS = "admin"
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"
DRIVER_PATH = "/opt/spark-jars/postgresql.jar"

@st.cache_resource
def get_spark_session():
    return SparkSession.builder \
        .appName("CasinoFraudSystem") \
        .config("spark.jars", DRIVER_PATH) \
        .config("spark.driver.extraClassPath", DRIVER_PATH) \
        .master("local[*]") \
        .getOrCreate()

def check_login(username, password):
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        cur.execute("SELECT password_hash, role FROM users WHERE username = %s", (username,))
        user_data = cur.fetchone()
        conn.close()
        
        if user_data:
            stored_hash = user_data[0].encode('utf-8')
            role = user_data[1]
            
            if bcrypt.checkpw(password.encode('utf-8'), stored_hash):
                return True, role
    except Exception as e:
        st.error(f"Błąd bazy: {e}")
    return False, None


st.title("System Wykrywania Fraudów Kasynowych")


if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.subheader("Logowanie")
    user = st.text_input("Użytkownik")
    pwd = st.text_input("Hasło", type="password")
    
    if st.button("Zaloguj"):
        is_valid, role = check_login(user, pwd)
        if is_valid:
            st.session_state.logged_in = True
            st.session_state.role = role
            st.session_state.user = user
            st.rerun()
        else:
            st.error("Błędne dane logowania!")

else:
    
    st.success(f"Zalogowano jako: {st.session_state.user} (Rola: {st.session_state.role})")
    
    if st.button("Wyloguj"):
        st.session_state.logged_in = False
        st.rerun()

    spark = get_spark_session()
    
    st.divider()
    
    
    st.header("1. Analiza Transakcji (Spark)")
    
    if st.button("Uruchom analizę fraudów"):
        with st.spinner('Przetwarzanie w klastrze Spark...'):
            df = spark.read \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "transactions") \
                .option("user", DB_USER) \
                .option("password", DB_PASS) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            # Logika biznesowa: wykrycie fraudów
            fraud_stats = df.groupBy("is_fraud").count().toPandas()
            avg_amount = df.select(avg("amount")).collect()[0][0]
            
            col1, col2 = st.columns(2)
            col1.metric("Średnia kwota transakcji", f"{avg_amount:.2f} PLN")
            col2.metric("Liczba wykrytych fraudów", int(fraud_stats[fraud_stats['is_fraud'] == True]['count'].iloc[0]))
            
            st.bar_chart(fraud_stats.set_index("is_fraud"))

    st.divider()

    st.header("2. Benchmark Wydajności (Big Data vs Tradycyjne)")
    st.caption("Porównanie czasu przetwarzania 100k+ rekordów")

    col_bench1, col_bench2 = st.columns(2)
    
    with col_bench1:
        if st.button("Test Spark (Rozproszony)"):
            start = time.time()
            # Spark Lazy Evaluation - wymuszamy akcję przez .count()
            df = spark.read.jdbc(url=JDBC_URL, table="transactions", properties={"user": DB_USER, "password": DB_PASS, "driver": "org.postgresql.Driver"})
            cnt = df.filter(col("amount") > 4000).count()
            end = time.time()
            st.info(f"Czas Spark: {end - start:.4f} s")
            st.write(f"Znaleziono: {cnt} rekordów")

    with col_bench2:
        if st.button("Test Pandas (Lokalny)"):
            start = time.time()
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            # Pandas pobiera wszystko do RAM
            pdf = pd.read_sql("SELECT * FROM transactions", conn)
            cnt = len(pdf[pdf['amount'] > 4000])
            conn.close()
            end = time.time()
            st.warning(f"Czas Pandas: {end - start:.4f} s")
            st.write(f"Znaleziono: {cnt} rekordów")
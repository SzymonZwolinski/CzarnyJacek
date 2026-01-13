import streamlit as st
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, count, stddev, abs, hour, desc, lag, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import psycopg2
import pandas as pd
import time
import bcrypt

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
        .appName("FraudAnalysisSystem") \
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

# --- UI Aplikacji ---
st.set_page_config(layout="wide", page_title="Apache Spark Analysis")

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.title("Login")
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
                st.error("Błąd logowania")

else:
    st.sidebar.title("Panel Sterowania")
    st.sidebar.info(f"Zalogowano: {st.session_state.user}")
    if st.sidebar.button("Wyloguj"):
        st.session_state.logged_in = False
        st.rerun()

    spark = get_spark_session()
    
    # Lazy Load danych
    df = spark.read.jdbc(url=JDBC_URL, table="transactions", properties={"user": DB_USER, "password": DB_PASS, "driver": "org.postgresql.Driver"})

    st.title("Analityka Apache Spark")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Statystyki (Z-Score)", 
        "Analiza Behawioralna (Window)", 
        "AI Clustering (MLlib)", 
        "Benchmark",
        "Podgląd Danych"
    ])

    # TAB 1: Z-SCORE (Statystyka)
    with tab1:
        st.header("Analiza Anomalii (Z-Score)")
        st.markdown("Wykrycie transakcji która rózni się (o 3 odchylenia standardowe) od średniej wszystkich transakcji.")
        
        if st.button("Oblicz Z-Score", key="zscore_btn"):
            with st.spinner("Liczenie w Sparku..."):
                stats = df.select(avg("amount").alias("mean"), stddev("amount").alias("std")).first()
                mean_val = stats["mean"]
                std_val = stats["std"]
                
                df_z = df.withColumn("z_score", abs((col("amount") - mean_val) / std_val))
                anomalies = df_z.filter(col("z_score") > 3)
                
                cnt = anomalies.count()
                st.metric("Znalezione anomalie", cnt, delta="Poważne naruszenia")
                st.dataframe(anomalies.select("id", "amount", "z_score").orderBy(desc("z_score")).limit(50).toPandas())

    # TAB 2: WINDOW FUNCTIONS (Skomplikowane)
    with tab2:
        st.header("Analiza 'Nagłego Skoku'")
        st.markdown("Porównuje transakcje do trzech ostatnich transakcji, aby wykryć nagłe wzrosty (np. Kradzież konta).")
        
        if st.button("Analizuj Historię Graczy", key="window_btn"):
            with st.spinner("Partycjonowanie i sortowanie danych..."):
                # Definicja okna: Partycja po User ID, sortowanie po dacie, patrzymy 3 wiersze wstecz
                w = Window.partitionBy("user_id").orderBy("transaction_date").rowsBetween(-3, -1)
                
                # Dodajemy kolumnę ze średnią z 3 ostatnich gier
                df_window = df.withColumn("avg_last_3", avg("amount").over(w))
                
                # Szukamy nagłych skoków (Current > 5 * History)
                suspicious = df_window.filter(col("amount") > (col("avg_last_3") * 5)) \
                                      .select("user_id", "amount", "avg_last_3", "transaction_date")
                
                result = suspicious.orderBy(desc("amount")).limit(100).toPandas()
                
                st.warning(f"Wykryto {len(result)} przypadków nagłego wzrostu stawek (Tilt/Kradzież konta).")
                st.dataframe(result.style.format({"amount": "{:.2f}", "avg_last_3": "{:.2f}"}))

    # TAB 3: MACHINE LEARNING (K-Means)
    with tab3:
        st.header("Nie nadzorowane uczenie maszynowe (K-Means)")
        st.markdown("Spark MLlib pozwala sztucznej inteligencji samodzielnie podzielic transakcje na grupy, dzielac transakcje na 3 grupy.")
        
        if st.button("Trenuj Model", key="ml_btn"):
            with st.spinner("Trenowanie modelu K-Means..."):
                # Przygotowanie danych dla ML (VectorAssembler)
                assembler = VectorAssembler(inputCols=["amount"], outputCol="features")
                df_features = assembler.transform(df)
                
                kmeans = KMeans().setK(3).setSeed(1)
                model = kmeans.fit(df_features)
                
                # Przypisanie predykcji
                predictions = model.transform(df_features)
                
                # Agregacja wyników
                summary = predictions.groupBy("prediction").agg(
                    count("*").alias("count"),
                    avg("amount").alias("avg_amount"),
                    stddev("amount").alias("std_dev")
                ).orderBy("avg_amount").toPandas()
                
                st.success("Model wytrenowany!")
                st.table(summary)
                
                st.caption("Legenda: prediction 0, 1, 2 to grupy odpowiednio Małe, Średnie, VIP/Podejrzenie.")

    # TAB 4: BENCHMARK
    with tab4:
        st.header("Test Wydajności (Spark a obliczenia klasyczne)")
        st.markdown("Porównanie czasu wykonania (odczyt z bazy + operacja).")

        # TEST 1: GRUPOWANIE I SORTOWANIE
        st.subheader("Test A: Agregacja Złożona (GroupBy + StdDev + Sort)")
        st.markdown("Sprawdzenie wydajności przy skomplikowanych operacjach na całych zbiorach danych")

        if st.button("Uruchom Test A"):
            # Spark
            start_s = time.time()
            # Spark musi przetasować dane (Shuffle)
            res_s = df.groupBy("user_id").agg(stddev("amount").alias("std")).orderBy(desc("std")).collect()
            time_s = time.time() - start_s
            
            # Pandas
            start_p = time.time()
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            pdf = pd.read_sql("SELECT user_id, amount FROM transactions", conn)
            # Pandas robi to w RAM
            res_p = pdf.groupby("user_id")["amount"].std().sort_values(ascending=False)
            conn.close()
            time_p = time.time() - start_p
            
            c1, c2 = st.columns(2)
            c1.metric("Spark", f"{time_s:.4f} s")
            c2.metric("Pandas", f"{time_p:.4f} s")

        st.divider()

        # TEST 2: FILTROWANIE
        st.subheader("Test B: Filtrowanie")
        st.markdown("Porównanie czasu wyszukiwania danych.")

        if st.button("Uruchom Test B"):
            # Spark
            start_s = time.time()
            # Spark push-down predicate (filtruje już przy odczycie jeśli sterownik pozwala)
            cnt_s = df.filter(col("amount") > 4000).count()
            time_s = time.time() - start_s
            
            # Pandas
            start_p = time.time()
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            pdf = pd.read_sql("SELECT amount FROM transactions", conn)
            cnt_p = len(pdf[pdf['amount'] > 4000])
            conn.close()
            time_p = time.time() - start_p
            
            c1, c2 = st.columns(2)
            c1.metric("Spark", f"{time_s:.4f} s")
            c2.metric("Pandas", f"{time_p:.4f} s")

        st.divider()

        # TEST 3: OBLICZENIA KOLUMNOWE
        st.subheader("Test C: Operacje Arytmetyczne")
        st.markdown("Wprowadzenie 19% podatku dla kazdego wiersza i oblizcenie sumy.")

        if st.button("Uruchom Test C"):
            # Spark
            start_s = time.time()
            # Wyliczamy 19% podatku dla każdego wiersza i liczymy sumę wpływów
            tax_s = df.withColumn("tax", col("amount") * 0.19).select(avg("tax")).collect()
            time_s = time.time() - start_s
            
            # Pandas
            start_p = time.time()
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
            pdf = pd.read_sql("SELECT amount FROM transactions", conn)
            # Wektoryzacja w Pandas (bardzo szybka dla prostych działań)
            tax_p = (pdf['amount'] * 0.19).mean()
            conn.close()
            time_p = time.time() - start_p
            
            c1, c2 = st.columns(2)
            c1.metric("Spark", f"{time_s:.4f} s")
            c2.metric("Pandas", f"{time_p:.4f} s")

    with tab5:
        st.header("Przegląd danych")
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM transactions")
        total_rows = cur.fetchone()[0]
        cur.close()
        
        PAGE_SIZE = 25
        total_pages = (total_rows // PAGE_SIZE) + (1 if total_rows % PAGE_SIZE > 0 else 0)
        
        col_ctrl, col_info = st.columns([1, 4])
        
        with col_ctrl:
            page = st.number_input("Strona", min_value=1, max_value=total_pages, value=1, step=1)
            
        with col_info:
            start_idx = (page - 1) * PAGE_SIZE + 1
            end_idx = min(page * PAGE_SIZE, total_rows)
            st.info(f"Wyświetlanie wierszy: **{start_idx} - {end_idx}** z {total_rows}")
        offset = (page - 1) * PAGE_SIZE
        query = f"SELECT * FROM transactions ORDER BY id LIMIT {PAGE_SIZE} OFFSET {offset}"
        
        df_page = pd.read_sql(query, conn)
        conn.close()
        
        st.dataframe(df_page, use_container_width=True)
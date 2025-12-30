from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round, avg
import time
import getpass
import pandas as pd
import matplotlib.pyplot as plt

def authenticate():
    print("--- SYSTEM ANALIZY KASYNA ---")
    user = input("Użytkownik: ")
    pwd = getpass.getpass("Hasło: ") 
    
    if user == "admin" and pwd == "admin":
        print("Dostęp przyznany.\n")
        return True
    else:
        print("Błąd logowania!")
        return True

if not authenticate():
    exit()

print("Inicjalizacja klastra Spark...")
spark = SparkSession.builder \
    .appName("BlackjackSecuritySystem") \
    .master("local[*]") \
    .getOrCreate()

sciezka = r".\blkjckhands.csv"

start_time = time.time()

df = spark.read.option("header", "true").option("inferSchema", "true").csv(sciezka)

df_clean = df.filter(col("winloss") != "Push")
df_clean = df_clean.withColumn("IsWin", when(col("winloss") == "Win", 1).otherwise(0))

statystyki = df_clean.groupBy("ply2cardsum") \
    .agg(
        count("*").alias("Liczba_Gier"),
        round(avg("IsWin") * 100, 2).alias("Procent_Wygranych")
    ) \
    .orderBy("ply2cardsum")

wyniki_pd = statystyki.toPandas()

end_time = time.time()
execution_time = end_time - start_time

print(f"\nAnalizę wykonano w czasie: {execution_time:.4f} sekundy.")
print("Wyniki:")
print(wyniki_pd.head(25))

print("\nGenerowanie wykresu...")
plt.figure(figsize=(10, 6))
plt.bar(wyniki_pd["ply2cardsum"], wyniki_pd["Procent_Wygranych"], color='green')
plt.title("Szansa wygranej w zależności od sumy kart (Blackjack)")
plt.xlabel("Suma kart (2-21)")
plt.ylabel("Procent wygranych (%)")
plt.grid(axis='y', linestyle='--', alpha=0.7)

output_img = "wykres_wydajnosci.png"
plt.savefig(output_img)
print(f"Wykres zapisano jako: {output_img}")

spark.stop()
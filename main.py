from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("CasinoFraudSystem") \
        .config("spark.jars", "/opt/spark-jars/postgresql.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark-jars/postgresql.jar") \
        .master("local[*]") \
        .getOrCreate()

def test_connection(spark):
    print("--- TEST POŁĄCZENIA Z BAZĄ ---")
    
    # Adres 'db' to nazwa serwisu z docker-compose
    jdbc_url = "jdbc:postgresql://db:5432/casino_db"
    
    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    try:
        df_users = spark.read.jdbc(url=jdbc_url, table="users", properties=properties)
        
        print("Pobrano dane z Postgresa:")
        df_users.show()
        print("Połączenie udane!")
        
    except Exception as e:
        print(f"Błąd połączenia: {e}")

if __name__ == "__main__":
    spark = create_spark_session()
    test_connection(spark)
    spark.stop()
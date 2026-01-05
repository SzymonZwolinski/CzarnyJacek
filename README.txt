# System Wykrywania oszustw (Spark + Streamlit)

Projekt demonstrujacy uzycie Big Data (Apache Spark) do analizy transakcji finansowych i wykrywania oszustw. 

## Instrukcja uruchomienia

1. Budowanie i uruchomienie kontenerow:
docker-compose up -d --build

2. Generowanie danych (100 000 transakcji):
docker-compose exec spark-app python generate_data.py

3. Konfiguracja hasla administratora:
docker-compose exec spark-app python fix_password.py

4. Uruchomienie aplikacji:
docker-compose exec spark-app streamlit run app.py --server.port 8501 --server.address 0.0.0.0

## Dostep do aplikacji

Adres: http://localhost:8501
Login: admin
Haslo: admin

## Opis plikow

- app.py: Glowna aplikacja w Streamlit. Laczy sie ze Sparkiem, przetwarza dane i wyswietla wyniki.
- Dockerfile: Konfiguracja srodowiska (Python, Java, Spark, sterowniki JDBC).
- docker-compose.yml: Konfiguracja uslugi bazy danych (PostgreSQL) i aplikacji.
- generate_data.py: Skrypt napelniajacy baze losowymi transakcjami.
- fix_password.py: Skrypt generujacy poprawny hash hasla dla uzytkownika admin (w przypadku jeżeli seed danych nie doda poprawnie hasła).
- init.sql: Skrypt tworzacy strukture bazy danych przy pierwszym uruchomieniu.

## Zatrzymywanie projektu

docker-compose down
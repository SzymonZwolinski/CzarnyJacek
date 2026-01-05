# JEŻELI CHCESZ ZRESETOWAĆ HASŁO UŻYTKOWNIKA 'admin' NA 'admin', URUCHOM TEN SKRYPT.
import psycopg2
import bcrypt
# Dane do połączenia (zgodne z docker-compose)
DB_HOST = "db"
DB_NAME = "casino_db"
DB_USER = "admin"
DB_PASS = "admin"

def reset_password():
    try:
        print("Łączenie z bazą...")
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()

        password_raw = b"admin"
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(password_raw, salt).decode('utf-8')

        cur.execute("UPDATE users SET password_hash = %s WHERE username = 'admin'", (hashed_password,))
        conn.commit()
        
        print("✅ SUKCES: Hasło dla użytkownika 'admin' zostało zresetowane na: 'admin'")
        
        cur.execute("SELECT password_hash FROM users WHERE username = 'admin'")
        new_hash = cur.fetchone()[0]
        print(f"Nowy hash w bazie: {new_hash}")

        conn.close()

    except Exception as e:
        print(f"❌ BŁĄD: {e}")

if __name__ == "__main__":
    reset_password()
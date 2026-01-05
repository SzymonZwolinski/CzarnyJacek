import psycopg2
import random
import time

def generate():
    conn = psycopg2.connect(
        dbname="casino_db", user="admin", password="admin", host="db", port="5432"
    )
    cur = conn.cursor()

    start = time.time()
    
    values = []
    for _ in range(100000):
        uid = 1  # dla admina
        amount = round(random.uniform(10.0, 5000.0), 2)
        is_fraud = amount > 4500
        values.append((uid, amount, is_fraud))

    args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode('utf-8') for x in values)
    cur.execute("INSERT INTO transactions (user_id, amount, is_fraud) VALUES " + args_str)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Gotowe! Czas: {time.time() - start:.2f}s")

if __name__ == "__main__":
    generate()
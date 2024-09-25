import psycopg2
import faker
import random
from datetime import datetime
fake = faker.Faker()

def generate_transaction():
    user = fake.simple_profile()
    return {
        "transactionId": fake.uuid4(),
        "userId": user["username"],
        "timestamp": datetime.utcnow().timestamp(),
        "amount": round(random.uniform(1, 1000), 2),
        "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
        "city": fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Crypto"]),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(["DISC10", "WELCOME20", "HOLIDAY15", "NEWYEAR25", "SUMMERSALE10"]),
        "affiliateId": fake.uuid4()
    }

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(255),
                timestamp BIGINT,
                amount NUMERIC(10, 2),
                currency VARCHAR(3),
                city VARCHAR(255),
                country VARCHAR(255),
                merchant_name VARCHAR(255),
                payment_method VARCHAR(255),
                ip_address VARCHAR(15),
                voucher_code VARCHAR(10),
                affiliate_id VARCHAR(36)
            )
        """)
        conn.commit()

def insert_data(conn, data):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO transactions (transaction_id, user_id, timestamp, amount, currency, city, country, merchant_name, payment_method, ip_address, voucher_code, affiliate_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data["transactionId"], data["userId"], data["timestamp"], data["amount"], data["currency"], data["city"], data["country"], data["merchantName"], data["paymentMethod"], data["ipAddress"], data["voucherCode"], data["affiliateId"]))
        conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname="financial_db",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    create_table(conn)

    transaction = generate_transaction()
    print(transaction)

    insert_data(conn, transaction)
    
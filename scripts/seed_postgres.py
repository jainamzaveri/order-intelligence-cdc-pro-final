import random
import time
from datetime import datetime, timedelta

from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

ENGINE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
fake = Faker()
random.seed(42)
Faker.seed(42)

CITY_CHOICES = ["New York", "San Francisco", "Chicago", "Austin", "Seattle", "Boston", "Atlanta", "Denver"]
LOYALTY_CHOICES = ["Bronze", "Silver", "Gold"]
PRODUCTS = [
    (1, "Noise-Canceling Headphones", "Audio", 199.0),
    (2, "Wireless Mouse", "Accessories", 35.0),
    (3, "Mechanical Keyboard", "Accessories", 120.0),
    (4, "4K Monitor", "Displays", 420.0),
    (5, "USB-C Hub", "Accessories", 65.0),
    (6, "Portable SSD", "Storage", 150.0),
    (7, "Laptop Stand", "Workspace", 48.0),
    (8, "Webcam Pro", "Video", 110.0),
    (9, "Bluetooth Speaker", "Audio", 88.0),
    (10, "Smart Lamp", "Workspace", 72.0),
    (11, "Gaming Chair", "Workspace", 299.0),
    (12, "Action Camera", "Video", 240.0),
    (13, "Tablet 11", "Computing", 380.0),
    (14, "Stylus Pen", "Computing", 45.0),
    (15, "Phone Gimbal", "Video", 95.0),
]


def wait_for_postgres(timeout_seconds: int = 120):
    start = time.time()
    while True:
        try:
            engine = create_engine(ENGINE_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except OperationalError:
            if time.time() - start > timeout_seconds:
                raise
            print("Waiting for Postgres on localhost:5432 ...")
            time.sleep(3)


def build_orders(customers, products):
    now = datetime.now().replace(microsecond=0)
    orders = []
    order_items = []
    payments = []
    order_item_id = 1

    for order_id in range(1, 401):
        customer_id = random.choice(customers)[0]
        order_ts = now - timedelta(hours=random.randint(1, 24 * 21), minutes=random.randint(0, 59))
        status = random.choices(
            ["placed", "shipped", "delivered", "canceled", "refunded"],
            weights=[16, 25, 44, 8, 7],
            k=1,
        )[0]

        line_count = random.randint(1, 4)
        selected_products = random.sample(products, k=line_count)
        total_amount = 0.0
        for product in selected_products:
            quantity = random.randint(1, 3)
            unit_price = float(product[3])
            line_amount = round(quantity * unit_price, 2)
            total_amount += line_amount
            order_items.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": product[0],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_amount": line_amount,
                }
            )
            order_item_id += 1

        total_amount = round(total_amount, 2)
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_ts": order_ts,
                "status": status,
                "total_amount": total_amount,
            }
        )

        if status == "canceled":
            payment_status = "voided"
            payment_amount = 0.0
        elif status == "refunded":
            payment_status = "refunded"
            payment_amount = total_amount
        else:
            payment_status = "paid"
            payment_amount = total_amount

        payments.append(
            {
                "payment_id": order_id,
                "order_id": order_id,
                "payment_ts": order_ts + timedelta(minutes=random.randint(1, 45)),
                "payment_status": payment_status,
                "amount": payment_amount,
            }
        )

    return orders, order_items, payments


def main():
    engine = wait_for_postgres()
    print("Connected to Postgres. Rebuilding source tables ...")

    customers = []
    for customer_id in range(1, 61):
        customers.append(
            (
                customer_id,
                fake.name(),
                random.choice(CITY_CHOICES),
                datetime.now().replace(microsecond=0) - timedelta(days=random.randint(30, 800)),
                random.choices(LOYALTY_CHOICES, weights=[50, 35, 15], k=1)[0],
            )
        )

    orders, order_items, payments = build_orders(customers, PRODUCTS)

    ddl = """
    DROP TABLE IF EXISTS payments;
    DROP TABLE IF EXISTS order_items;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS customers;

    CREATE TABLE customers (
        customer_id INT PRIMARY KEY,
        customer_name TEXT NOT NULL,
        city TEXT NOT NULL,
        signup_ts TIMESTAMP NOT NULL,
        loyalty_tier TEXT NOT NULL
    );

    CREATE TABLE products (
        product_id INT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        unit_price NUMERIC(10,2) NOT NULL
    );

    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT NOT NULL,
        order_ts TIMESTAMP NOT NULL,
        status TEXT NOT NULL,
        total_amount NUMERIC(10,2) NOT NULL
    );

    CREATE TABLE order_items (
        order_item_id INT PRIMARY KEY,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        unit_price NUMERIC(10,2) NOT NULL,
        line_amount NUMERIC(10,2) NOT NULL
    );

    CREATE TABLE payments (
        payment_id INT PRIMARY KEY,
        order_id INT NOT NULL,
        payment_ts TIMESTAMP NOT NULL,
        payment_status TEXT NOT NULL,
        amount NUMERIC(10,2) NOT NULL
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))

        for row in customers:
            conn.execute(
                text(
                    """
                    INSERT INTO customers (customer_id, customer_name, city, signup_ts, loyalty_tier)
                    VALUES (:customer_id, :customer_name, :city, :signup_ts, :loyalty_tier)
                    """
                ),
                {
                    "customer_id": row[0],
                    "customer_name": row[1],
                    "city": row[2],
                    "signup_ts": row[3],
                    "loyalty_tier": row[4],
                },
            )

        for product in PRODUCTS:
            conn.execute(
                text(
                    """
                    INSERT INTO products (product_id, product_name, category, unit_price)
                    VALUES (:product_id, :product_name, :category, :unit_price)
                    """
                ),
                {
                    "product_id": product[0],
                    "product_name": product[1],
                    "category": product[2],
                    "unit_price": product[3],
                },
            )

        for row in orders:
            conn.execute(
                text(
                    """
                    INSERT INTO orders (order_id, customer_id, order_ts, status, total_amount)
                    VALUES (:order_id, :customer_id, :order_ts, :status, :total_amount)
                    """
                ),
                row,
            )

        for row in order_items:
            conn.execute(
                text(
                    """
                    INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, line_amount)
                    VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :line_amount)
                    """
                ),
                row,
            )

        for row in payments:
            conn.execute(
                text(
                    """
                    INSERT INTO payments (payment_id, order_id, payment_ts, payment_status, amount)
                    VALUES (:payment_id, :order_id, :payment_ts, :payment_status, :amount)
                    """
                ),
                row,
            )

    print("Source data ready.")
    print(f"Customers: {len(customers)}")
    print(f"Products: {len(PRODUCTS)}")
    print(f"Orders: {len(orders)}")
    print(f"Order items: {len(order_items)}")
    print(f"Payments: {len(payments)}")


if __name__ == "__main__":
    main()

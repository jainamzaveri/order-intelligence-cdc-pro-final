import random
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

ENGINE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
engine = create_engine(ENGINE_URL)
random.seed(123)


def scalar(conn, sql: str) -> int:
    return int(conn.execute(text(sql)).scalar() or 0)


def main() -> None:
    with engine.begin() as conn:
        next_order_id = scalar(conn, "SELECT COALESCE(MAX(order_id), 0) + 1 FROM orders")
        next_order_item_id = scalar(conn, "SELECT COALESCE(MAX(order_item_id), 0) + 1 FROM order_items")
        next_payment_id = scalar(conn, "SELECT COALESCE(MAX(payment_id), 0) + 1 FROM payments")

        customer_ids = [row[0] for row in conn.execute(text("SELECT customer_id FROM customers ORDER BY customer_id")).fetchall()]
        product_rows = conn.execute(text("SELECT product_id, unit_price FROM products ORDER BY product_id")).fetchall()

        created = 0
        for i in range(12):
            customer_id = random.choice(customer_ids)
            order_id = next_order_id + i
            order_ts = datetime.now().replace(microsecond=0) - timedelta(minutes=random.randint(0, 90))
            line_count = random.randint(1, 3)
            selected = random.sample(product_rows, k=line_count)
            total_amount = 0.0
            for product_id, unit_price in selected:
                quantity = random.randint(1, 2)
                line_amount = round(float(unit_price) * quantity, 2)
                total_amount += line_amount
                conn.execute(
                    text(
                        """
                        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, line_amount)
                        VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :line_amount)
                        """
                    ),
                    {
                        "order_item_id": next_order_item_id,
                        "order_id": order_id,
                        "product_id": int(product_id),
                        "quantity": quantity,
                        "unit_price": float(unit_price),
                        "line_amount": line_amount,
                    },
                )
                next_order_item_id += 1

            total_amount = round(total_amount, 2)
            conn.execute(
                text(
                    """
                    INSERT INTO orders (order_id, customer_id, order_ts, status, total_amount)
                    VALUES (:order_id, :customer_id, :order_ts, :status, :total_amount)
                    """
                ),
                {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_ts": order_ts,
                    "status": "placed",
                    "total_amount": total_amount,
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO payments (payment_id, order_id, payment_ts, payment_status, amount)
                    VALUES (:payment_id, :order_id, :payment_ts, :payment_status, :amount)
                    """
                ),
                {
                    "payment_id": next_payment_id,
                    "order_id": order_id,
                    "payment_ts": order_ts + timedelta(minutes=5),
                    "payment_status": "paid",
                    "amount": total_amount,
                },
            )
            next_payment_id += 1
            created += 1

        conn.execute(text("UPDATE orders SET status = 'shipped' WHERE order_id IN (SELECT order_id FROM orders ORDER BY order_ts DESC LIMIT 5)"))
        conn.execute(text("UPDATE orders SET status = 'delivered' WHERE order_id IN (SELECT order_id FROM orders WHERE status = 'shipped' ORDER BY order_ts DESC LIMIT 5)"))
        conn.execute(text("UPDATE orders SET status = 'refunded' WHERE order_id IN (SELECT order_id FROM orders WHERE status = 'delivered' ORDER BY random() LIMIT 3)"))
        conn.execute(text("UPDATE payments SET payment_status = 'refunded' WHERE order_id IN (SELECT order_id FROM orders WHERE status = 'refunded')"))

        conn.execute(text("UPDATE customers SET loyalty_tier = 'Gold' WHERE customer_id IN (SELECT customer_id FROM customers ORDER BY random() LIMIT 2)"))
        conn.execute(text("UPDATE products SET unit_price = unit_price * 1.08 WHERE product_id IN (2, 5, 9)"))

    print(f"Triggered CDC changes. New orders inserted: {created}")
    print("Also updated order statuses, payment statuses, customer tiers, and selected product prices.")


if __name__ == "__main__":
    main()

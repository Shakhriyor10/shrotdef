import sqlite3
from datetime import datetime, timedelta
from typing import Iterable, Optional

DB_PATH = "bot.sqlite3"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER UNIQUE NOT NULL,
                first_name TEXT,
                last_name TEXT,
                phone TEXT,
                created_at TEXT NOT NULL,
                last_active TEXT NOT NULL,
                activity_count INTEGER NOT NULL DEFAULT 0,
                is_blocked INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price_per_kg REAL NOT NULL,
                description TEXT,
                is_deleted INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity TEXT NOT NULL,
                address TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                order_price_per_kg REAL,
                closed_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            )
            """
        )
        try:
            conn.execute(
                "ALTER TABLE orders ADD COLUMN status TEXT NOT NULL DEFAULT 'open'"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE orders ADD COLUMN order_price_per_kg REAL")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE orders ADD COLUMN closed_at TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE orders ADD COLUMN closed_by INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE orders ADD COLUMN canceled_by_role TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE orders ADD COLUMN latitude REAL")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE orders ADD COLUMN longitude REAL")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE users ADD COLUMN activity_count INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE users ADD COLUMN is_blocked INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE products ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        conn.execute("UPDATE orders SET status = 'open' WHERE status IS NULL")
        conn.execute(
            """
            UPDATE orders
            SET order_price_per_kg = (
                SELECT price_per_kg FROM products WHERE products.id = orders.product_id
            )
            WHERE order_price_per_kg IS NULL
            """
        )
        conn.execute(
            """
            UPDATE orders
            SET canceled_by_role = CASE
                WHEN closed_by IS NULL THEN 'user'
                ELSE 'admin'
            END
            WHERE status = 'canceled' AND canceled_by_role IS NULL
            """
        )


def add_or_update_user(tg_id: int, first_name: str, last_name: Optional[str]) -> None:
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM users WHERE tg_id = ?", (tg_id,)
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE users
                SET first_name = ?, last_name = ?, last_active = ?
                WHERE tg_id = ?
                """,
                (first_name, last_name, now, tg_id),
            )
        else:
            conn.execute(
                """
                INSERT INTO users (tg_id, first_name, last_name, created_at, last_active)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tg_id, first_name, last_name, now, now),
            )


def update_user_phone(tg_id: int, phone: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE users SET phone = ? WHERE tg_id = ?",
            (phone, tg_id),
        )


def update_last_active(tg_id: int) -> None:
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE users
            SET last_active = ?,
                activity_count = COALESCE(activity_count, 0) + 1
            WHERE tg_id = ?
            """,
            (now, tg_id),
        )


def get_user_by_tg_id(tg_id: int) -> Optional[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE tg_id = ?", (tg_id,)
        ).fetchone()


def set_user_blocked(tg_id: int, blocked: bool) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE users SET is_blocked = ? WHERE tg_id = ?",
            (1 if blocked else 0, tg_id),
        )


def is_user_blocked(tg_id: int) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT is_blocked FROM users WHERE tg_id = ?",
            (tg_id,),
        ).fetchone()
    return bool(row and row["is_blocked"])


def list_users() -> Iterable[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute("SELECT * FROM users").fetchall()


def add_product(name: str, price_per_kg: float, description: Optional[str]) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO products (name, price_per_kg, description) VALUES (?, ?, ?)",
            (name, price_per_kg, description),
        )
        return int(cur.lastrowid)


def update_product(product_id: int, name: str, price_per_kg: float, description: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE products
            SET name = ?, price_per_kg = ?, description = ?
            WHERE id = ?
            """,
            (name, price_per_kg, description, product_id),
        )


def update_product_name(product_id: int, name: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE products SET name = ? WHERE id = ?",
            (name, product_id),
        )


def update_product_price(product_id: int, price_per_kg: float) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE products SET price_per_kg = ? WHERE id = ?",
            (price_per_kg, product_id),
        )


def update_product_description(product_id: int, description: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE products SET description = ? WHERE id = ?",
            (description, product_id),
        )


def set_product_photos(product_id: int, file_ids: list[str]) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM product_photos WHERE product_id = ?", (product_id,))
        for position, file_id in enumerate(file_ids):
            conn.execute(
                """
                INSERT INTO product_photos (product_id, file_id, position)
                VALUES (?, ?, ?)
                """,
                (product_id, file_id, position),
            )


def list_products() -> Iterable[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT * FROM products WHERE is_deleted = 0 ORDER BY id DESC"
        ).fetchall()


def get_product(product_id: int) -> Optional[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT * FROM products WHERE id = ? AND is_deleted = 0",
            (product_id,),
        ).fetchone()


def delete_product(product_id: int) -> bool:
    with get_connection() as conn:
        conn.execute("DELETE FROM product_photos WHERE product_id = ?", (product_id,))
        cur = conn.execute(
            "UPDATE products SET is_deleted = 1 WHERE id = ? AND is_deleted = 0",
            (product_id,),
        )
        return cur.rowcount > 0


def get_product_photos(product_id: int) -> list[str]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT file_id FROM product_photos WHERE product_id = ? ORDER BY position",
            (product_id,),
        ).fetchall()
    return [row["file_id"] for row in rows]


def add_order(
    user_id: int,
    product_id: int,
    quantity: str,
    address: str,
    order_price_per_kg: float,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> int:
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO orders (
                user_id,
                product_id,
                quantity,
                address,
                latitude,
                longitude,
                created_at,
                status,
                order_price_per_kg
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?)
            """,
            (
                user_id,
                product_id,
                quantity,
                address,
                latitude,
                longitude,
                now,
                order_price_per_kg,
            ),
        )
        return int(cur.lastrowid)


def add_manual_user(name: str, phone: str, admin_id: int) -> int:
    now = datetime.utcnow().isoformat()
    base_id = -int(datetime.utcnow().timestamp() * 1000) * 1000 - admin_id
    for offset in range(5):
        tg_id = base_id - offset
        try:
            with get_connection() as conn:
                cur = conn.execute(
                    """
                    INSERT INTO users (tg_id, first_name, last_name, phone, created_at, last_active)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (tg_id, name, None, phone, now, now),
                )
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            continue
    raise RuntimeError("Failed to create a manual user record.")


def add_admin_order(
    user_id: int,
    product_id: int,
    quantity: str,
    address: str,
    order_price_per_kg: float,
    admin_id: int,
) -> int:
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO orders (
                user_id,
                product_id,
                quantity,
                address,
                created_at,
                status,
                order_price_per_kg,
                closed_at,
                closed_by
            )
            VALUES (?, ?, ?, ?, ?, 'closed', ?, ?, ?)
            """,
            (
                user_id,
                product_id,
                quantity,
                address,
                now,
                order_price_per_kg,
                now,
                admin_id,
            ),
        )
        return int(cur.lastrowid)


def update_order_status(
    order_id: int,
    new_status: str,
    admin_id: int,
) -> tuple[bool, Optional[str], Optional[int], Optional[str]]:
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT status, closed_by, canceled_by_role FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
        if not row:
            return False, None, None, None
        current_status = row["status"]
        if current_status != "open":
            return False, current_status, row["closed_by"], row["canceled_by_role"]
        canceled_by_role = "admin" if new_status == "canceled" else None
        conn.execute(
            """
            UPDATE orders
            SET status = ?, closed_at = ?, closed_by = ?, canceled_by_role = ?
            WHERE id = ? AND status = 'open'
            """,
            (new_status, now, admin_id, canceled_by_role, order_id),
        )
        return True, new_status, admin_id, canceled_by_role


def cancel_order_by_user(
    order_id: int,
    user_id: int,
) -> tuple[bool, Optional[str], Optional[str]]:
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT status, canceled_by_role
            FROM orders
            WHERE id = ? AND user_id = ?
            """,
            (order_id, user_id),
        ).fetchone()
        if not row:
            return False, None, None
        current_status = row["status"]
        if current_status != "open":
            return False, current_status, row["canceled_by_role"]
        conn.execute(
            """
            UPDATE orders
            SET status = 'canceled',
                closed_at = ?,
                closed_by = NULL,
                canceled_by_role = 'user'
            WHERE id = ? AND user_id = ? AND status = 'open'
            """,
            (now, order_id, user_id),
        )
        return True, "canceled", "user"


def count_orders() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM orders").fetchone()
    return int(row["total"])


def count_orders_by_status(status: str) -> int:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM orders WHERE status = ?",
            (status,),
        ).fetchone()
    return int(row["total"])


def list_orders_with_details(
    status: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> Iterable[sqlite3.Row]:
    query = """
        SELECT
            orders.id,
            orders.quantity,
            orders.address,
            orders.latitude,
            orders.longitude,
            orders.created_at,
            orders.status,
            orders.order_price_per_kg,
            orders.canceled_by_role,
            users.first_name,
            users.last_name,
            users.phone,
            products.name AS product_name,
            products.price_per_kg AS product_price_per_kg
        FROM orders
        JOIN users ON orders.user_id = users.id
        JOIN products ON orders.product_id = products.id
    """
    params: list[object] = []
    if status:
        query += " WHERE orders.status = ?"
        params.append(status)
    query += " ORDER BY orders.created_at DESC"
    if limit is not None:
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    with get_connection() as conn:
        return conn.execute(query, params).fetchall()


def list_orders_for_report(
    start_at: str,
    end_at: str,
) -> Iterable[sqlite3.Row]:
    query = """
        SELECT
            orders.id,
            orders.quantity,
            orders.created_at,
            orders.order_price_per_kg,
            users.id AS user_id,
            users.first_name,
            users.last_name,
            users.phone,
            products.price_per_kg AS product_price_per_kg
        FROM orders
        JOIN users ON orders.user_id = users.id
        JOIN products ON orders.product_id = products.id
        WHERE orders.status = 'closed'
          AND date(COALESCE(orders.closed_at, orders.created_at)) >= date(?)
          AND date(COALESCE(orders.closed_at, orders.created_at)) <= date(?)
        ORDER BY orders.created_at ASC
    """
    with get_connection() as conn:
        return conn.execute(query, (start_at, end_at)).fetchall()


def get_order_with_details(order_id: int) -> Optional[sqlite3.Row]:
    query = """
        SELECT
            orders.id,
            orders.quantity,
            orders.address,
            orders.latitude,
            orders.longitude,
            orders.created_at,
            orders.status,
            orders.order_price_per_kg,
            orders.closed_by,
            orders.canceled_by_role,
            users.first_name,
            users.last_name,
            users.phone,
            products.name AS product_name,
            products.price_per_kg AS product_price_per_kg
        FROM orders
        JOIN users ON orders.user_id = users.id
        JOIN products ON orders.product_id = products.id
        WHERE orders.id = ?
    """
    with get_connection() as conn:
        return conn.execute(query, (order_id,)).fetchone()


def list_orders_for_user(user_id: int) -> Iterable[sqlite3.Row]:
    query = """
        SELECT
            orders.id,
            orders.quantity,
            orders.address,
            orders.latitude,
            orders.longitude,
            orders.created_at,
            orders.status,
            orders.order_price_per_kg,
            orders.canceled_by_role,
            products.name AS product_name,
            products.price_per_kg AS product_price_per_kg
        FROM orders
        JOIN products ON orders.product_id = products.id
        WHERE orders.user_id = ?
        ORDER BY orders.created_at DESC
    """
    with get_connection() as conn:
        return conn.execute(query, (user_id,)).fetchall()


def count_users() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM users").fetchone()
    return int(row["total"])


def count_active_users(days: int) -> int:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM users WHERE last_active >= ?",
            (cutoff,),
        ).fetchone()
    return int(row["total"])


def list_top_purchasers(limit: int = 100) -> Iterable[sqlite3.Row]:
    query = """
        SELECT
            users.first_name,
            users.last_name,
            users.phone,
            COUNT(orders.id) AS order_count
        FROM orders
        JOIN users ON orders.user_id = users.id
        WHERE orders.status = 'closed'
        GROUP BY users.id
        ORDER BY order_count DESC, users.id DESC
        LIMIT ?
    """
    with get_connection() as conn:
        return conn.execute(query, (limit,)).fetchall()


def list_top_active_users(limit: int = 100) -> Iterable[sqlite3.Row]:
    query = """
        SELECT
            first_name,
            last_name,
            phone,
            activity_count,
            last_active
        FROM users
        ORDER BY activity_count DESC, last_active DESC
        LIMIT ?
    """
    with get_connection() as conn:
        return conn.execute(query, (limit,)).fetchall()
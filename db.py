import re
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Iterable, Optional

DB_PATH = "bot.sqlite3"


def get_tashkent_tz() -> timezone:
    try:
        return ZoneInfo("Asia/Tashkent")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=5))


TASHKENT_TZ = get_tashkent_tz()


def now_tashkent() -> datetime:
    return datetime.now(TASHKENT_TZ)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_phone(value: str) -> str:
    digits = "".join(char for char in str(value or "") if char.isdigit())
    if digits.startswith("998") and len(digits) >= 12:
        return digits[-9:]
    return digits


def init_db() -> None:
    now = now_tashkent().isoformat()
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                quantity_tons REAL NOT NULL,
                total_amount REAL NOT NULL,
                created_at TEXT NOT NULL,
                created_by INTEGER,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS product_stock (
                product_id INTEGER PRIMARY KEY,
                quantity_tons REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cashbox (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                amount REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS order_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL,
                created_by INTEGER,
                FOREIGN KEY(order_id) REFERENCES orders(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cashbox_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_type TEXT NOT NULL,
                amount REAL NOT NULL,
                reason TEXT NOT NULL,
                reference_type TEXT,
                reference_id INTEGER,
                note TEXT,
                created_at TEXT NOT NULL,
                created_by INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse_receipt_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receipt_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL,
                created_by INTEGER,
                FOREIGN KEY(receipt_id) REFERENCES warehouse_receipts(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                position TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by INTEGER,
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                employee_id INTEGER,
                amount REAL NOT NULL,
                comment TEXT,
                created_at TEXT NOT NULL,
                created_by INTEGER,
                FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE SET NULL
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
                "ALTER TABLE users ADD COLUMN created_at TEXT NOT NULL DEFAULT ''"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE users ADD COLUMN last_active TEXT NOT NULL DEFAULT ''"
            )
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
        conn.execute(
            "UPDATE users SET created_at = ? WHERE created_at IS NULL OR created_at = ''",
            (now,),
        )
        conn.execute(
            "UPDATE users SET last_active = COALESCE(NULLIF(last_active, ''), created_at, ?)",
            (now,),
        )
        conn.execute(
            "INSERT OR IGNORE INTO cashbox (id, amount, updated_at) VALUES (1, 0, ?)",
            (now,),
        )

    merge_all_users_by_phone()


def list_employees() -> Iterable[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT id, name, position, created_at
            FROM employees
            WHERE is_active = 1
            ORDER BY id DESC
            """
        ).fetchall()


def add_employee(name: str, position: str, created_by: Optional[int]) -> int:
    clean_name = str(name or "").strip()
    clean_position = str(position or "").strip()
    if not clean_name:
        raise ValueError("name_required")
    if not clean_position:
        raise ValueError("position_required")

    now = now_tashkent().isoformat()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO employees (name, position, created_at, created_by)
            VALUES (?, ?, ?, ?)
            """,
            (clean_name, clean_position, now, created_by),
        )
        return int(cur.lastrowid)


def list_expenses_paginated(
    page: int = 1,
    page_size: int = 50,
    start_date: str = "",
    end_date: str = "",
) -> tuple[list[sqlite3.Row], int]:
    safe_page = max(int(page or 1), 1)
    safe_page_size = max(min(int(page_size or 50), 200), 1)
    offset = (safe_page - 1) * safe_page_size

    clauses: list[str] = []
    params: list[object] = []
    if start_date:
        clauses.append("date(expenses.created_at) >= date(?)")
        params.append(start_date)
    if end_date:
        clauses.append("date(expenses.created_at) <= date(?)")
        params.append(end_date)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_connection() as conn:
        total_row = conn.execute(
            f"SELECT COUNT(*) AS total FROM expenses {where_sql}",
            tuple(params),
        ).fetchone()
        rows = conn.execute(
            f"""
            SELECT
                expenses.id,
                expenses.category,
                expenses.employee_id,
                expenses.amount,
                expenses.comment,
                expenses.created_at,
                employees.name AS employee_name,
                employees.position AS employee_position
            FROM expenses
            LEFT JOIN employees ON employees.id = expenses.employee_id
            {where_sql}
            ORDER BY datetime(expenses.created_at) DESC, expenses.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*params, safe_page_size, offset]),
        ).fetchall()

    return rows, int(total_row["total"] if total_row else 0)


def add_expense(
    category: str,
    amount: float,
    created_by: Optional[int],
    employee_id: Optional[int] = None,
    comment: str = "",
) -> int:
    clean_category = str(category or "").strip().lower()
    if clean_category not in {"salary", "other"}:
        raise ValueError("invalid_category")
    if amount <= 0:
        raise ValueError("invalid_amount")

    clean_comment = str(comment or "").strip()
    now = now_tashkent().isoformat()

    with get_connection() as conn:
        safe_employee_id = None
        if clean_category == "salary":
            if not employee_id:
                raise ValueError("employee_required")
            employee_row = conn.execute(
                "SELECT id, name, position FROM employees WHERE id = ? AND is_active = 1",
                (employee_id,),
            ).fetchone()
            if not employee_row:
                raise LookupError("employee_not_found")
            safe_employee_id = int(employee_row["id"])
            if not clean_comment:
                clean_comment = (
                    f"Ish haqi: {employee_row['name']} ({employee_row['position']})"
                )
        elif not clean_comment:
            raise ValueError("comment_required")

        cur = conn.execute(
            """
            INSERT INTO expenses (category, employee_id, amount, comment, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (clean_category, safe_employee_id, amount, clean_comment, now, created_by),
        )

        conn.execute(
            "UPDATE cashbox SET amount = amount - ?, updated_at = ? WHERE id = 1",
            (amount, now),
        )
        _add_cashbox_operation(
            conn,
            operation_type="expense",
            amount=amount,
            reason="salary_expense" if clean_category == "salary" else "other_expense",
            note=clean_comment,
            reference_type="expense",
            reference_id=int(cur.lastrowid),
            created_at=now,
            created_by=created_by,
        )
        return int(cur.lastrowid)
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
        conn.execute(
            "UPDATE users SET created_at = ? WHERE created_at IS NULL OR created_at = ''",
            (now,),
        )
        conn.execute(
            "UPDATE users SET last_active = COALESCE(NULLIF(last_active, ''), created_at, ?)",
            (now,),
        )
        conn.execute(
            "INSERT OR IGNORE INTO cashbox (id, amount, updated_at) VALUES (1, 0, ?)",
            (now,),
        )

    merge_all_users_by_phone()


def add_or_update_user(tg_id: int, first_name: str, last_name: Optional[str]) -> None:
    now = now_tashkent().isoformat()
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
    now = now_tashkent().isoformat()
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


def get_user_by_id(user_id: int) -> Optional[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()


def search_users(query: str, limit: int = 10) -> Iterable[sqlite3.Row]:
    normalized = (query or "").strip()
    if not normalized:
        return []
    pattern = f"%{normalized.lower()}%"
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT id, tg_id, first_name, last_name, phone
            FROM users
            WHERE lower(COALESCE(first_name, '')) LIKE ?
               OR lower(COALESCE(last_name, '')) LIKE ?
               OR lower(COALESCE(phone, '')) LIKE ?
            ORDER BY last_active DESC
            LIMIT ?
            """,
            (pattern, pattern, pattern, limit),
        ).fetchall()


def find_user_by_phone(phone: str) -> Optional[sqlite3.Row]:
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM users WHERE phone IS NOT NULL AND phone != ''"
        ).fetchall()
    for row in rows:
        if normalize_phone(row["phone"] or "") == normalized:
            return row
    return None


def _choose_primary_user_id(rows: list[sqlite3.Row]) -> int:
    def sort_key(row: sqlite3.Row) -> tuple[int, int, int]:
        tg_id = int(row["tg_id"])
        is_real_tg = 1 if tg_id > 0 else 0
        activity = int(row["activity_count"] or 0)
        return (is_real_tg, activity, -int(row["id"]))

    return int(max(rows, key=sort_key)["id"])


def _merge_users_into_target(conn: sqlite3.Connection, target_id: int, source_ids: list[int]) -> None:
    for source_id in source_ids:
        if source_id == target_id:
            continue
        conn.execute(
            "UPDATE orders SET user_id = ? WHERE user_id = ?",
            (target_id, source_id),
        )
        conn.execute("DELETE FROM users WHERE id = ?", (source_id,))


def merge_all_users_by_phone() -> int:
    merged_count = 0
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, tg_id, phone, activity_count FROM users WHERE phone IS NOT NULL AND phone != ''"
        ).fetchall()
        groups: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            normalized = normalize_phone(row["phone"] or "")
            if not normalized:
                continue
            groups.setdefault(normalized, []).append(row)

        for group_rows in groups.values():
            if len(group_rows) < 2:
                continue
            target_id = _choose_primary_user_id(group_rows)
            source_ids = [int(row["id"]) for row in group_rows if int(row["id"]) != target_id]
            _merge_users_into_target(conn, target_id, source_ids)
            merged_count += len(source_ids)

    return merged_count


def merge_users_by_phone(tg_id: int, phone: str) -> None:
    normalized = normalize_phone(phone)
    if not normalized:
        return
    with get_connection() as conn:
        target = conn.execute(
            "SELECT id FROM users WHERE tg_id = ?",
            (tg_id,),
        ).fetchone()
        if not target:
            return
        target_id = int(target["id"])
        group_rows = conn.execute(
            "SELECT id, phone FROM users WHERE id != ? AND phone IS NOT NULL AND phone != ''",
            (target_id,),
        ).fetchall()
        source_ids = [
            int(row["id"])
            for row in group_rows
            if normalize_phone(row["phone"] or "") == normalized
        ]

        _merge_users_into_target(conn, target_id, source_ids)


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



def set_product_stock_tons(product_id: int, quantity_tons: float) -> None:
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO product_stock (product_id, quantity_tons, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                quantity_tons = excluded.quantity_tons,
                updated_at = excluded.updated_at
            """,
            (product_id, quantity_tons, now),
        )

def add_order(
    user_id: int,
    product_id: int,
    quantity: str,
    address: str,
    order_price_per_kg: float,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> int:
    now = now_tashkent().isoformat()
    qty_tons = parse_quantity_to_tons(quantity)
    if qty_tons is None or qty_tons <= 0:
        raise ValueError("Invalid quantity")
    with get_connection() as conn:
        stock_row = conn.execute(
            "SELECT quantity_tons FROM product_stock WHERE product_id = ?",
            (product_id,),
        ).fetchone()
        available_tons = float(stock_row["quantity_tons"] if stock_row else 0)
        if available_tons + 1e-9 < qty_tons:
            raise ValueError("Not enough stock")
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
        conn.execute(
            """
            INSERT INTO product_stock (product_id, quantity_tons, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                quantity_tons = quantity_tons - excluded.quantity_tons,
                updated_at = excluded.updated_at
            """,
            (product_id, qty_tons, now),
        )
        return int(cur.lastrowid)


def add_manual_user(name: str, phone: str, admin_id: int) -> int:
    now = now_tashkent().isoformat()
    base_id = -int(now_tashkent().timestamp() * 1000) * 1000 - admin_id
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
    now = now_tashkent().isoformat()
    qty_tons = parse_quantity_to_tons(quantity)
    if qty_tons is None or qty_tons <= 0:
        raise ValueError("Invalid quantity")
    with get_connection() as conn:
        stock_row = conn.execute(
            "SELECT quantity_tons FROM product_stock WHERE product_id = ?",
            (product_id,),
        ).fetchone()
        available_tons = float(stock_row["quantity_tons"] if stock_row else 0)
        if available_tons + 1e-9 < qty_tons:
            raise ValueError("Not enough stock")
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
        conn.execute(
            """
            INSERT INTO product_stock (product_id, quantity_tons, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                quantity_tons = quantity_tons - excluded.quantity_tons,
                updated_at = excluded.updated_at
            """,
            (product_id, qty_tons, now),
        )
        total_amount = qty_tons * 1000 * float(order_price_per_kg)
        conn.execute(
            "UPDATE cashbox SET amount = amount + ?, updated_at = ? WHERE id = 1",
            (total_amount, now),
        )
        order_id = int(cur.lastrowid)
        _add_cashbox_operation(
            conn,
            operation_type="income",
            amount=total_amount,
            reason="closed_order",
            reference_type="order",
            reference_id=order_id,
            created_by=admin_id,
            note="Buyurtma darhol yopildi",
            created_at=now,
        )
        return order_id




def _add_cashbox_operation(
    conn: sqlite3.Connection,
    *,
    operation_type: str,
    amount: float,
    reason: str,
    reference_type: Optional[str] = None,
    reference_id: Optional[int] = None,
    created_by: Optional[int] = None,
    note: Optional[str] = None,
    created_at: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO cashbox_operations (
            operation_type,
            amount,
            reason,
            reference_type,
            reference_id,
            note,
            created_at,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            operation_type,
            amount,
            reason,
            reference_type,
            reference_id,
            note,
            created_at or now_tashkent().isoformat(),
            created_by,
        ),
    )
def update_order_status(
    order_id: int,
    new_status: str,
    admin_id: int,
) -> tuple[bool, Optional[str], Optional[int], Optional[str]]:
    now = now_tashkent().isoformat()
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
        order_row = conn.execute(
            "SELECT product_id, quantity FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
        conn.execute(
            """
            UPDATE orders
            SET status = ?, closed_at = ?, closed_by = ?, canceled_by_role = ?
            WHERE id = ? AND status = 'open'
            """,
            (new_status, now, admin_id, canceled_by_role, order_id),
        )
        if new_status == "canceled" and order_row:
            qty_tons = parse_quantity_to_tons(order_row["quantity"])
            if qty_tons and qty_tons > 0:
                conn.execute(
                    """
                    INSERT INTO product_stock (product_id, quantity_tons, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(product_id) DO UPDATE SET
                        quantity_tons = quantity_tons + excluded.quantity_tons,
                        updated_at = excluded.updated_at
                    """,
                    (order_row["product_id"], qty_tons, now),
                )
        return True, new_status, admin_id, canceled_by_role


def cancel_order_by_user(
    order_id: int,
    user_id: int,
) -> tuple[bool, Optional[str], Optional[str]]:
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT status, canceled_by_role, product_id, quantity
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
        qty_tons = parse_quantity_to_tons(row["quantity"])
        if qty_tons and qty_tons > 0:
            conn.execute(
                """
                INSERT INTO product_stock (product_id, quantity_tons, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    quantity_tons = quantity_tons + excluded.quantity_tons,
                    updated_at = excluded.updated_at
                """,
                (row["product_id"], qty_tons, now),
            )
        return True, "canceled", "user"


def parse_quantity_to_tons(value: str) -> Optional[float]:
    cleaned = (value or "").strip().lower().replace(",", ".")
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", cleaned)
    if not match:
        return None
    number = float(match.group(1))
    if "kg" in cleaned:
        return number / 1000
    return number


def list_stock_products() -> Iterable[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT
                p.id,
                p.name,
                p.price_per_kg,
                COALESCE(ps.quantity_tons, 0) AS quantity_tons
            FROM products p
            LEFT JOIN product_stock ps ON ps.product_id = p.id
            WHERE p.is_deleted = 0
            ORDER BY p.name ASC
            """
        ).fetchall()


def add_stock_receipt(product_id: int, quantity_tons: float, total_amount: float, created_by: Optional[int]) -> int:
    if quantity_tons <= 0:
        raise ValueError("Quantity must be > 0")
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO warehouse_receipts (product_id, quantity_tons, total_amount, created_at, created_by)
            VALUES (?, ?, ?, ?, ?)
            """,
            (product_id, quantity_tons, total_amount, now, created_by),
        )
        conn.execute(
            """
            INSERT INTO product_stock (product_id, quantity_tons, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                quantity_tons = quantity_tons + excluded.quantity_tons,
                updated_at = excluded.updated_at
            """,
            (product_id, quantity_tons, now),
        )
        return int(cur.lastrowid)


def _build_warehouse_receipts_filters(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    payment_filter: str = "all",
    remaining_filter: str = "all",
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if start_date:
        clauses.append("date(wr.created_at) >= date(?)")
        params.append(start_date)
    if end_date:
        clauses.append("date(wr.created_at) <= date(?)")
        params.append(end_date)

    if payment_filter == "paid":
        clauses.append("COALESCE(payments.paid_amount, 0) >= wr.total_amount")
    elif payment_filter == "unpaid":
        clauses.append("COALESCE(payments.paid_amount, 0) <= 0")
    elif payment_filter == "partial":
        clauses.append("COALESCE(payments.paid_amount, 0) > 0")
        clauses.append("COALESCE(payments.paid_amount, 0) < wr.total_amount")

    if remaining_filter == "with_remaining":
        clauses.append("(wr.total_amount - COALESCE(payments.paid_amount, 0)) > 0")
    elif remaining_filter == "no_remaining":
        clauses.append("(wr.total_amount - COALESCE(payments.paid_amount, 0)) <= 0")

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def list_warehouse_receipts(
    limit: int = 10,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    payment_filter: str = "all",
    remaining_filter: str = "all",
) -> Iterable[sqlite3.Row]:
    where_sql, params = _build_warehouse_receipts_filters(
        start_date=start_date,
        end_date=end_date,
        payment_filter=payment_filter,
        remaining_filter=remaining_filter,
    )
    query = f"""
        SELECT
            wr.id,
            wr.product_id,
            wr.quantity_tons,
            wr.total_amount,
            wr.created_at,
            wr.created_by,
            p.name AS product_name,
            COALESCE(payments.paid_amount, 0) AS paid_amount,
            COALESCE(payments.payments_count, 0) AS payments_count
        FROM warehouse_receipts wr
        JOIN products p ON p.id = wr.product_id
        LEFT JOIN (
            SELECT receipt_id, SUM(amount) AS paid_amount, COUNT(*) AS payments_count
            FROM warehouse_receipt_payments
            GROUP BY receipt_id
        ) AS payments ON payments.receipt_id = wr.id
        {where_sql}
        ORDER BY datetime(wr.created_at) DESC, wr.id DESC
        LIMIT ? OFFSET ?
    """
    with get_connection() as conn:
        return conn.execute(query, [*params, limit, offset]).fetchall()


def count_warehouse_receipts(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    payment_filter: str = "all",
    remaining_filter: str = "all",
) -> int:
    where_sql, params = _build_warehouse_receipts_filters(
        start_date=start_date,
        end_date=end_date,
        payment_filter=payment_filter,
        remaining_filter=remaining_filter,
    )
    query = f"""
        SELECT COUNT(*) AS total
        FROM warehouse_receipts wr
        LEFT JOIN (
            SELECT receipt_id, SUM(amount) AS paid_amount
            FROM warehouse_receipt_payments
            GROUP BY receipt_id
        ) AS payments ON payments.receipt_id = wr.id
        {where_sql}
    """
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
    return int(row["total"] if row else 0)


def add_warehouse_receipt_payment(receipt_id: int, amount: float, created_by: Optional[int]) -> int:
    if amount <= 0:
        raise ValueError("Amount must be > 0")
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        receipt_row = conn.execute(
            "SELECT id, total_amount FROM warehouse_receipts WHERE id = ?",
            (receipt_id,),
        ).fetchone()
        if not receipt_row:
            raise LookupError("Receipt not found")
        paid_row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS paid FROM warehouse_receipt_payments WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        paid = float(paid_row["paid"] if paid_row else 0)
        remaining = float(receipt_row["total_amount"] or 0) - paid
        if amount - remaining > 1e-9:
            raise ValueError("Payment exceeds remaining amount")

        cur = conn.execute(
            """
            INSERT INTO warehouse_receipt_payments (receipt_id, amount, created_at, created_by)
            VALUES (?, ?, ?, ?)
            """,
            (receipt_id, amount, now, created_by),
        )
        payment_id = int(cur.lastrowid)
        conn.execute(
            "UPDATE cashbox SET amount = amount - ?, updated_at = ? WHERE id = 1",
            (amount, now),
        )
        _add_cashbox_operation(
            conn,
            operation_type="expense",
            amount=amount,
            reason="warehouse_receipt_payment",
            reference_type="warehouse_receipt_payment",
            reference_id=payment_id,
            created_by=created_by,
            note=f"Sklad prihodi #{receipt_id} uchun to'lov",
            created_at=now,
        )
        return payment_id


def list_warehouse_receipt_payments(receipt_id: int) -> Iterable[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT id, receipt_id, amount, created_at, created_by
            FROM warehouse_receipt_payments
            WHERE receipt_id = ?
            ORDER BY datetime(created_at) DESC, id DESC
            """,
            (receipt_id,),
        ).fetchall()


def delete_warehouse_receipt_payment(receipt_id: int, payment_id: int, deleted_by: Optional[int]) -> bool:
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, amount FROM warehouse_receipt_payments WHERE id = ? AND receipt_id = ?",
            (payment_id, receipt_id),
        ).fetchone()
        if not row:
            return False
        amount = float(row["amount"])
        conn.execute("DELETE FROM warehouse_receipt_payments WHERE id = ?", (payment_id,))
        conn.execute(
            "UPDATE cashbox SET amount = amount + ?, updated_at = ? WHERE id = 1",
            (amount, now),
        )
        _add_cashbox_operation(
            conn,
            operation_type="income",
            amount=amount,
            reason="warehouse_receipt_payment_revert",
            reference_type="warehouse_receipt_payment",
            reference_id=payment_id,
            created_by=deleted_by,
            note=f"Sklad prihodi #{receipt_id} to'lovi bekor qilindi",
            created_at=now,
        )
        return True


def delete_warehouse_receipt(receipt_id: int) -> tuple[bool, str]:
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        receipt_row = conn.execute(
            """
            SELECT id, product_id, quantity_tons
            FROM warehouse_receipts
            WHERE id = ?
            """,
            (receipt_id,),
        ).fetchone()
        if not receipt_row:
            return False, "not_found"

        payment_count_row = conn.execute(
            "SELECT COUNT(*) AS total FROM warehouse_receipt_payments WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        payment_count = int(payment_count_row["total"] if payment_count_row else 0)
        if payment_count > 0:
            return False, "has_payments"

        product_id = int(receipt_row["product_id"])
        receipt_qty = float(receipt_row["quantity_tons"] or 0)
        stock_row = conn.execute(
            "SELECT COALESCE(quantity_tons, 0) AS quantity_tons FROM product_stock WHERE product_id = ?",
            (product_id,),
        ).fetchone()
        current_stock = float(stock_row["quantity_tons"] if stock_row else 0)

        if current_stock + 1e-9 < receipt_qty:
            return False, "insufficient_stock"

        conn.execute(
            """
            INSERT INTO product_stock (product_id, quantity_tons, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
                quantity_tons = quantity_tons - excluded.quantity_tons,
                updated_at = excluded.updated_at
            """,
            (product_id, receipt_qty, now),
        )
        conn.execute("DELETE FROM warehouse_receipts WHERE id = ?", (receipt_id,))
        return True, "ok"


def get_cashbox_amount() -> float:
    with get_connection() as conn:
        row = conn.execute("SELECT amount FROM cashbox WHERE id = 1").fetchone()
    return float(row["amount"] if row else 0)


def set_cashbox_amount(amount: float) -> None:
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        current_row = conn.execute("SELECT amount FROM cashbox WHERE id = 1").fetchone()
        current_amount = float(current_row["amount"] if current_row else 0)
        conn.execute(
            "UPDATE cashbox SET amount = ?, updated_at = ? WHERE id = 1",
            (amount, now),
        )
        delta = amount - current_amount
        if abs(delta) > 1e-9:
            _add_cashbox_operation(
                conn,
                operation_type="income" if delta > 0 else "expense",
                amount=abs(delta),
                reason="manual_adjustment",
                note="Kassa qo'lda o'zgartirildi",
                created_at=now,
            )



def list_cashbox_operations(limit: int = 20) -> Iterable[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT
                id,
                operation_type,
                amount,
                reason,
                reference_type,
                reference_id,
                note,
                created_at,
                created_by
            FROM cashbox_operations
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


def list_cashbox_operations_paginated(
    page: int = 1,
    page_size: int = 50,
    start_date: str = "",
    end_date: str = "",
) -> tuple[list[sqlite3.Row], int]:
    safe_page = max(int(page or 1), 1)
    safe_page_size = max(min(int(page_size or 50), 200), 1)
    offset = (safe_page - 1) * safe_page_size

    clauses: list[str] = []
    params: list[object] = []
    if start_date:
        clauses.append("date(created_at) >= date(?)")
        params.append(start_date)
    if end_date:
        clauses.append("date(created_at) <= date(?)")
        params.append(end_date)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_connection() as conn:
        total_row = conn.execute(
            f"SELECT COUNT(*) AS total FROM cashbox_operations {where_sql}",
            tuple(params),
        ).fetchone()
        rows = conn.execute(
            f"""
            SELECT
                id,
                operation_type,
                amount,
                reason,
                reference_type,
                reference_id,
                note,
                created_at,
                created_by
            FROM cashbox_operations
            {where_sql}
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*params, safe_page_size, offset]),
        ).fetchall()

    return rows, int(total_row["total"] if total_row else 0)


def add_order_payment(order_id: int, amount: float, created_by: Optional[int]) -> int:
    if amount <= 0:
        raise ValueError("Amount must be > 0")
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        order_row = conn.execute(
            """
            SELECT
                orders.status,
                orders.quantity,
                COALESCE(orders.order_price_per_kg, products.price_per_kg) AS price_per_kg
            FROM orders
            JOIN products ON products.id = orders.product_id
            WHERE orders.id = ?
            """,
            (order_id,),
        ).fetchone()
        if not order_row:
            raise LookupError("Order not found")
        if order_row["status"] != "closed":
            raise RuntimeError("Order must be closed")

        qty_tons = parse_quantity_to_tons(order_row["quantity"])
        total_amount = float((qty_tons or 0) * 1000 * float(order_row["price_per_kg"] or 0))
        paid_row = conn.execute(
            "SELECT COALESCE(SUM(amount), 0) AS paid FROM order_payments WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        paid = float(paid_row["paid"] if paid_row else 0)
        remaining = total_amount - paid
        if amount - remaining > 1e-9:
            raise ValueError("Payment exceeds remaining amount")

        cur = conn.execute(
            """
            INSERT INTO order_payments (order_id, amount, created_at, created_by)
            VALUES (?, ?, ?, ?)
            """,
            (order_id, amount, now, created_by),
        )
        payment_id = int(cur.lastrowid)
        conn.execute(
            "UPDATE cashbox SET amount = amount + ?, updated_at = ? WHERE id = 1",
            (amount, now),
        )
        _add_cashbox_operation(
            conn,
            operation_type="income",
            amount=amount,
            reason="order_payment",
            reference_type="order_payment",
            reference_id=payment_id,
            created_by=created_by,
            note=f"Buyurtma #{order_id} uchun to'lov",
            created_at=now,
        )
        return payment_id


def list_order_payments(order_id: int) -> Iterable[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute(
            """
            SELECT id, order_id, amount, created_at, created_by
            FROM order_payments
            WHERE order_id = ?
            ORDER BY datetime(created_at) DESC, id DESC
            """,
            (order_id,),
        ).fetchall()


def delete_order_payment(order_id: int, payment_id: int, deleted_by: Optional[int]) -> bool:
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, amount FROM order_payments WHERE id = ? AND order_id = ?",
            (payment_id, order_id),
        ).fetchone()
        if not row:
            return False
        amount = float(row["amount"])
        conn.execute("DELETE FROM order_payments WHERE id = ?", (payment_id,))
        conn.execute(
            "UPDATE cashbox SET amount = amount - ?, updated_at = ? WHERE id = 1",
            (amount, now),
        )
        _add_cashbox_operation(
            conn,
            operation_type="expense",
            amount=amount,
            reason="order_payment_revert",
            reference_type="order_payment",
            reference_id=payment_id,
            created_by=deleted_by,
            note=f"Buyurtma #{order_id} to'lovi bekor qilindi",
            created_at=now,
        )
        return True

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
            products.price_per_kg AS product_price_per_kg,
            COALESCE(payments.paid_amount, 0) AS paid_amount,
            COALESCE(payments.payments_count, 0) AS payments_count
        FROM orders
        JOIN users ON orders.user_id = users.id
        JOIN products ON orders.product_id = products.id
        LEFT JOIN (
            SELECT order_id, SUM(amount) AS paid_amount, COUNT(*) AS payments_count
            FROM order_payments
            GROUP BY order_id
        ) AS payments ON payments.order_id = orders.id
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
            products.name AS product_name,
            products.price_per_kg AS product_price_per_kg,
            COALESCE(payments.paid_amount, 0) AS paid_amount
        FROM orders
        JOIN users ON orders.user_id = users.id
        JOIN products ON orders.product_id = products.id
        LEFT JOIN (
            SELECT order_id, SUM(amount) AS paid_amount
            FROM order_payments
            GROUP BY order_id
        ) AS payments ON payments.order_id = orders.id
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


def delete_order(order_id: int) -> bool:
    with get_connection() as conn:
        cur = conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))
        return cur.rowcount > 0


def admin_delete_order(order_id: int, admin_id: Optional[int]) -> tuple[bool, str]:
    now = now_tashkent().isoformat()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                o.id,
                o.status,
                o.canceled_by_role,
                o.product_id,
                o.quantity,
                COALESCE(p.payments_count, 0) AS payments_count
            FROM orders o
            LEFT JOIN (
                SELECT order_id, COUNT(*) AS payments_count
                FROM order_payments
                GROUP BY order_id
            ) p ON p.order_id = o.id
            WHERE o.id = ?
            """,
            (order_id,),
        ).fetchone()
        if not row:
            return False, "not_found"

        payments_count = int(row["payments_count"] or 0)
        if payments_count > 0:
            return False, "has_payments"

        status = row["status"]
        canceled_by_role = row["canceled_by_role"]

        if status == "closed":
            qty_tons = parse_quantity_to_tons(row["quantity"])
            if qty_tons and qty_tons > 0:
                conn.execute(
                    """
                    INSERT INTO product_stock (product_id, quantity_tons, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(product_id) DO UPDATE SET
                        quantity_tons = quantity_tons + excluded.quantity_tons,
                        updated_at = excluded.updated_at
                    """,
                    (row["product_id"], qty_tons, now),
                )
            conn.execute(
                """
                UPDATE orders
                SET status = 'canceled',
                    closed_at = ?,
                    closed_by = ?,
                    canceled_by_role = 'admin'
                WHERE id = ?
                """,
                (now, admin_id, order_id),
            )
            return True, "closed_marked_canceled"

        if status == "open":
            qty_tons = parse_quantity_to_tons(row["quantity"])
            if qty_tons and qty_tons > 0:
                conn.execute(
                    """
                    INSERT INTO product_stock (product_id, quantity_tons, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(product_id) DO UPDATE SET
                        quantity_tons = quantity_tons + excluded.quantity_tons,
                        updated_at = excluded.updated_at
                    """,
                    (row["product_id"], qty_tons, now),
                )
            conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))
            return True, "open_deleted"

        if status == "canceled" and canceled_by_role == "admin":
            conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))
            return True, "admin_canceled_deleted"

        return False, "not_allowed"


def list_orders_for_user(user_id: int, limit: Optional[int] = None, offset: int = 0) -> Iterable[sqlite3.Row]:
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
            products.price_per_kg AS product_price_per_kg,
            COALESCE(payments.paid_amount, 0) AS paid_amount,
            COALESCE(payments.payments_count, 0) AS payments_count
        FROM orders
        JOIN products ON orders.product_id = products.id
        LEFT JOIN (
            SELECT order_id, SUM(amount) AS paid_amount, COUNT(*) AS payments_count
            FROM order_payments
            GROUP BY order_id
        ) AS payments ON payments.order_id = orders.id
        WHERE orders.user_id = ?
        ORDER BY orders.created_at DESC
    """
    params: list[object] = [user_id]
    if limit is not None:
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    with get_connection() as conn:
        return conn.execute(query, params).fetchall()



def count_orders_for_user(user_id: int) -> int:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM orders WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return int(row["total"] if row else 0)

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

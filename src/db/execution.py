from .manager import DbManager
import sqlite3

class ExecutionDbManager(DbManager):
    def __init__(self):
        super().__init__(f"{exchange.value}_{asset_pair.replace('-', '_')}_executions.db")

    @property
    def create_query(self):
        return """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                transaction_type TEXT NOT NULL CHECK (transaction_type IN ('buy', 'sell', 'none')),
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                total_position REAL NOT NULL
            )
        """

    @property
    def retrieve_query(self):
        return """
            SELECT * FROM transactions
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        """

    @property
    def desired_columns(self):
        return [
            'transaction_id', 'timestamp', 'transaction_type', 
            'quantity', 'price', 'total_position'
        ]

    def record_transaction(self, timestamp, transaction_type, quantity, price, total_position):
        """Records a new transaction into the database."""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO transactions (timestamp, transaction_type, quantity, price, total_position)
                VALUES (?, ?, ?, ?, ?)
            """, (timestamp.isoformat(), transaction_type, quantity, price, total_position))
            conn.commit()
            print("Transaction recorded successfully.")

    def record_transaction(self, timestamp, transaction_type, quantity, price, total_position):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO transactions (timestamp, transaction_type, quantity, price, total_position)
            VALUES (?, ?, ?, ?, ?)
        """, (timestamp, transaction_type, quantity, price, total_position))
        self.conn.commit()

"""init_schema.py - Tạo keyspace và các bảng Cassandra từ file schema.cql."""
import re
from pathlib import Path

from common import KEYSPACE, connect_cassandra


def main():
    session = connect_cassandra()  # chưa chọn keyspace vì có thể chưa tồn tại
    cql = (Path(__file__).parent / "schema.cql").read_text(encoding="utf-8")
    cql = re.sub(r"--.*$", "", cql, flags=re.MULTILINE)  # bỏ comment
    statements = [s.strip() for s in cql.split(";") if s.strip()]

    for stmt in statements:
        session.execute(stmt)
        print("OK:", " ".join(stmt.split())[:75], "...")

    session.cluster.shutdown()
    print(f"\nĐã chạy {len(statements)} câu lệnh. Keyspace '{KEYSPACE}' sẵn sàng.")


if __name__ == "__main__":
    main()

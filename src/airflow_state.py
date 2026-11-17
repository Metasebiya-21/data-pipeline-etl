from airflow.models import Variable
from airflow.utils.session import create_session
from sqlalchemy.exc import IntegrityError

try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except Exception:
    pg_insert = None


def upsert_variable(key: str, value: str) -> None:
    """Create or update an Airflow Variable without failing on concurrent inserts."""
    with create_session() as session:
        dialect = session.get_bind().dialect.name
        if pg_insert and dialect == "postgresql":
            stmt = pg_insert(Variable).values(key=key, val=value, description=None, is_encrypted=False)
            stmt = stmt.on_conflict_do_update(
                index_elements=["key"],
                set_={"val": value, "description": None, "is_encrypted": False},
            )
            session.execute(stmt)
            session.flush()
            return

        try:
            Variable.set(key, value, session=session)
        except IntegrityError:
            session.rollback()
            session.query(Variable).filter(Variable.key == key).update(
                {"val": value, "description": None, "is_encrypted": False}
            )
            session.flush()

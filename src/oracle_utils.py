from typing import Any

from src.settings import Settings


_oracledb_inited = False


def get_oracle_module(settings: Settings) -> Any:
    mode = settings.oracle_connect_mode.lower()
    if mode == "oracledb":
        import oracledb

        _init_oracle_client(oracledb, settings)
        return oracledb
    if mode == "cx_oracle":
        import cx_Oracle

        return cx_Oracle

    try:
        import oracledb

        _init_oracle_client(oracledb, settings)
        return oracledb
    except Exception:
        import cx_Oracle

        return cx_Oracle


def _init_oracle_client(oracledb_module: Any, settings: Settings) -> None:
    global _oracledb_inited
    if _oracledb_inited:
        return
    if settings.oracle_client_lib_dir:
        oracledb_module.init_oracle_client(lib_dir=settings.oracle_client_lib_dir)
    _oracledb_inited = True


def get_oracle_connection(settings: Settings) -> Any:
    oracle = get_oracle_module(settings)
    return oracle.connect(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=settings.oracle_dsn,
    )

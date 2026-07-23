from dataclasses import dataclass
from typing import Optional, Callable
from airflow.exceptions import AirflowException # type: ignore

@dataclass
class DatabaseHookConfig:
    database_type: str = ""
    hook_class: Optional[Callable] = None

    def __post_init__(self):
        self._resolve_hook_class()

    def _resolve_hook_class(self) -> None:
        if self.hook_class is not None:
            return
        # Skip resolution if database_type is not yet set
        if not self.database_type:
            return
        
        try:
            if self.database_type == "mssql":
                from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook # type: ignore
                self.hook_class = MsSqlHook

            elif self.database_type == "mysql":
                from airflow.providers.mysql.hooks.mysql import MySqlHook # type: ignore
                self.hook_class = MySqlHook

            elif self.database_type in ("mongo", "mongodb"):
                try:
                    from airflow.providers.mongo.hooks.mongo import MongoHook  # type: ignore
                    self.hook_class = MongoHook
                except ImportError:
                    # Optional provider; pipeline uses pymongo via MongoDBConnectionFactory.
                    self.hook_class = None

            elif self.database_type == "clickhouse":
                from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook # type: ignore
                self.hook_class = ClickHouseHook

            else:
                raise AirflowException(
                    f"Unsupported database_type: {self.database_type}"
                )

        except ImportError as exc:
            raise AirflowException(
                f"Required Airflow provider for {self.database_type} is not installed."
            ) from exc

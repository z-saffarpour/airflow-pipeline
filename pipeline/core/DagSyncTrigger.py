"""Trigger store master-data sync orchestrators for replication inconsistencies."""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


_ACTIVE_DAG_RUN_STATES = frozenset(
    {"queued", "scheduled", "deferred", "running", "restarting", "up_for_retry"}
)
_SUCCESS_DAG_RUN_STATES = frozenset({"success"})
_FAILED_DAG_RUN_STATES = frozenset({"failed", "upstream_failed"})
_EXECUTION_MODE_PARALLEL = "parallel"
_EXECUTION_MODE_SERIAL = "serial"


class DagSyncTrigger:
    """
    Trigger downstream sync orchestrator DAGs for stores with replication gaps.

    Uses deterministic run IDs and active-run checks to prevent duplicate triggers.
    Optionally waits for triggered DagRuns to reach a terminal state before returning.

    execution_mode:
        - "serial": trigger each DAG, wait for completion, then trigger the next
        - "parallel": trigger all DAGs, then wait for all to complete
    """

    def __init__(
        self,
        orchestrator_dag_ids: Optional[Iterable[str]] = None,
        parent_run_id: Optional[str] = None,
        logical_date: Optional[datetime] = None,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        execution_mode: str = _EXECUTION_MODE_SERIAL,
        failed_dag_run_retries: int = 1,
        failed_dag_run_retry_delay: int = 60,
    ) -> None:
        if execution_mode not in {_EXECUTION_MODE_PARALLEL, _EXECUTION_MODE_SERIAL}:
            raise ValueError(
                f"execution_mode must be '{_EXECUTION_MODE_PARALLEL}' or "
                f"'{_EXECUTION_MODE_SERIAL}', got {execution_mode!r}"
            )
        if failed_dag_run_retries < 0:
            raise ValueError("failed_dag_run_retries must be >= 0")
        if failed_dag_run_retry_delay < 0:
            raise ValueError("failed_dag_run_retry_delay must be >= 0")

        self.orchestrator_dag_ids = list(orchestrator_dag_ids or [])
        self.parent_run_id = parent_run_id or "manual"
        self.logical_date = logical_date or datetime.now(timezone.utc)
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self.execution_mode = execution_mode
        self.failed_dag_run_retries = failed_dag_run_retries
        self.failed_dag_run_retry_delay = failed_dag_run_retry_delay
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def resolve_logical_date(context: Optional[dict] = None) -> datetime:
        """
        Resolve a timezone-aware logical_date from a parent task context.

        Falls back to current UTC time when the parent run has no logical date
        (common for manual/API-triggered DAG runs in Airflow 3).
        """
        if context:
            from pipeline.core.ExecutionDateExtractor import ExecutionDateExtractor

            logical_dt = ExecutionDateExtractor.resolve_logical_datetime(context)
            if logical_dt:
                return logical_dt

        return datetime.now(timezone.utc)

    @staticmethod
    def _use_task_sdk() -> bool:
        """Return True when running under Airflow 3 Task SDK (no direct ORM access)."""
        try:
            from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS  # type: ignore # noqa: F401

            return True
        except ImportError:
            return False
    
    @staticmethod
    def build_run_id(store_number: Any, dag_id: str, parent_run_id: str) -> str:
        """Build a deterministic DAG run ID for idempotent triggering."""
        safe_store = str(store_number).strip().replace(" ", "_")
        safe_dag_id = str(dag_id).strip().replace(" ", "_")
        return f"{safe_store}_{safe_dag_id}_{parent_run_id}"

    @staticmethod
    def logical_date_for_store(store_number: Any, dag_id: str) -> datetime:
        """
        Return a unique logical_date per store and downstream DAG.

        Sharing one logical_date across many manual triggers of the same DAG can
        prevent Airflow from scheduling more than one run; a small per-store offset
        keeps run_id idempotency while allowing parallel DagRuns.
        """
        seed = f"{store_number}:{dag_id}"
        offset_us = abs(hash(seed)) % 999_999
        return datetime.now(timezone.utc) + timedelta(microseconds=offset_us)

    @staticmethod
    def build_conf(store_number: Any) -> Dict[str, str]:
        """
        Build trigger conf payload.

        Includes both Store_number (requirement) and store_number (orchestrator convention).
        """
        normalized_store = str(store_number).strip()
        return {
            "Store_number": normalized_store,
            "store_number": normalized_store,
        }

    @staticmethod
    def _is_dag_run_not_found(exc: Exception) -> bool:
        """Return True when GetDagRunState reports the run does not exist yet."""
        try:
            from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType  # type: ignore
        except ImportError:
            return False

        if not isinstance(exc, AirflowRuntimeError):
            return False

        err = exc.error
        if err.error != ErrorType.API_SERVER_ERROR:
            return False

        detail = err.detail or {}
        if detail.get("status_code") != 404:
            return False

        api_detail = detail.get("detail") or {}
        if isinstance(api_detail, dict) and "detail" in api_detail:
            api_detail = api_detail["detail"]
        return isinstance(api_detail, dict) and api_detail.get("reason") == "not_found"

    def _get_existing_run_state(self, dag_id: str, run_id: str) -> Optional[str]:
        if self._use_task_sdk():
            return self._get_existing_run_state_sdk(dag_id, run_id)
        return self._get_existing_run_state_orm(dag_id, run_id)

    def _get_existing_run_state_sdk(self, dag_id: str, run_id: str) -> Optional[str]:
        from airflow.sdk.execution_time.comms import DagRunStateResult, GetDagRunState  # type: ignore
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS  # type: ignore

        try:
            comms_msg = SUPERVISOR_COMMS.send(
                msg=GetDagRunState(dag_id=dag_id, run_id=run_id),
            )
            if isinstance(comms_msg, DagRunStateResult):
                return comms_msg.state
        except Exception as exc:
            if self._is_dag_run_not_found(exc):
                self.logger.debug(
                    "[DagSyncTrigger._get_existing_run_state_sdk] No existing DagRun for %s/%s; safe to trigger",
                    dag_id,
                    run_id,
                )
                return None
            self.logger.warning(
                "[DagSyncTrigger._get_existing_run_state_sdk] Could not check existing DagRun for %s/%s: %s",
                dag_id,
                run_id,
                exc,
            )
        return None

    def _get_existing_run_state_orm(self, dag_id: str, run_id: str) -> Optional[str]:
        try:
            from airflow.models import DagRun  # type: ignore
            from airflow.utils.session import create_session  # type: ignore

            with create_session() as session:
                dag_run = (
                    session.query(DagRun)
                    .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
                    .one_or_none()
                )
                return dag_run.state if dag_run else None
        except Exception as exc:
            self.logger.warning(
                "[DagSyncTrigger._get_existing_run_state_orm] Could not check existing DagRun for %s/%s: %s",
                dag_id,
                run_id,
                exc,
            )
            return None

    def _apply_wait_failures(
        self,
        failed: List[Dict[str, str]],
        wait_result: Dict[str, Any],
    ) -> None:
        if wait_result.get("all_success"):
            return

        for run in wait_result.get("failed_runs", []):
            failed.append(
                {
                    "dag_id": run["dag_id"],
                    "error": (
                        f"DagRun {run['run_id']} finished with state={run['state']}"
                    ),
                }
            )

    def _trigger_store_dag(
        self,
        normalized_store: str,
        dag_id: str,
        conf: Dict[str, str],
    ) -> Dict[str, Any]:
        """Trigger, skip, or fail a single downstream DAG for one store."""
        run_id = self.build_run_id(normalized_store, dag_id, self.parent_run_id)
        logical_date = self.logical_date_for_store(normalized_store, dag_id)
        existing_state = self._get_existing_run_state(dag_id, run_id)

        if existing_state in _ACTIVE_DAG_RUN_STATES:
            self.logger.info(
                "[DagSyncTrigger._trigger_store_dag] Skipping duplicate trigger for store=%s dag=%s run_id=%s state=%s",
                normalized_store,
                dag_id,
                run_id,
                existing_state,
            )
            return {
                "dag_id": dag_id,
                "run_id": run_id,
                "action": "skipped",
            }

        reset_dag_run = existing_state in {"failed", "success"}

        try:
            self._trigger_dag(
                dag_id,
                run_id,
                conf,
                logical_date=logical_date,
                reset_dag_run=reset_dag_run,
            )
            self.logger.info(
                "[DagSyncTrigger._trigger_store_dag] Triggered sync orchestrator dag=%s store=%s run_id=%s logical_date=%s reset=%s",
                dag_id,
                normalized_store,
                run_id,
                logical_date.isoformat(),
                reset_dag_run,
            )
            return {
                "dag_id": dag_id,
                "run_id": run_id,
                "action": "triggered",
            }
        except Exception as exc:
            self.logger.error(
                "[DagSyncTrigger._trigger_store_dag] Failed to trigger dag=%s for store=%s: %s",
                dag_id,
                normalized_store,
                exc,
                exc_info=True,
            )
            return {
                "dag_id": dag_id,
                "run_id": run_id,
                "action": "failed",
                "error": str(exc),
            }

    def _trigger_store_parallel(
        self,
        normalized_store: str,
        conf: Dict[str, str],
    ) -> Dict[str, Any]:
        triggered: List[str] = []
        skipped: List[str] = []
        failed: List[Dict[str, str]] = []
        runs_to_wait: List[Tuple[str, str]] = []

        for dag_id in self.orchestrator_dag_ids:
            outcome = self._trigger_store_dag(normalized_store, dag_id, conf)
            dag_id = outcome["dag_id"]
            run_id = outcome["run_id"]

            if outcome["action"] == "skipped":
                skipped.append(dag_id)
                runs_to_wait.append((dag_id, run_id))
            elif outcome["action"] == "triggered":
                triggered.append(dag_id)
                runs_to_wait.append((dag_id, run_id))
            else:
                failed.append(
                    {"dag_id": dag_id, "error": outcome.get("error", "trigger failed")}
                )

        wait_result: Optional[Dict[str, Any]] = None
        if self.wait_for_completion and runs_to_wait and not failed:
            wait_result = self._wait_for_dag_runs_with_retry(
                runs_to_wait,
                conf,
                normalized_store,
            )
            self._apply_wait_failures(failed, wait_result)

        return {
            "triggered_dags": triggered,
            "skipped_dags": skipped,
            "failed_dags": failed,
            "monitored_runs": [
                {"dag_id": dag_id, "run_id": run_id}
                for dag_id, run_id in runs_to_wait
            ],
            "wait_result": wait_result,
        }

    def _trigger_store_serial(
        self,
        normalized_store: str,
        conf: Dict[str, str],
    ) -> Dict[str, Any]:
        triggered: List[str] = []
        skipped: List[str] = []
        failed: List[Dict[str, str]] = []
        runs_to_wait: List[Tuple[str, str]] = []
        wait_results: List[Dict[str, Any]] = []

        for dag_id in self.orchestrator_dag_ids:
            outcome = self._trigger_store_dag(normalized_store, dag_id, conf)
            dag_id = outcome["dag_id"]
            run_id = outcome["run_id"]

            if outcome["action"] == "skipped":
                skipped.append(dag_id)
            elif outcome["action"] == "triggered":
                triggered.append(dag_id)
            else:
                failed.append(
                    {"dag_id": dag_id, "error": outcome.get("error", "trigger failed")}
                )
                break

            runs_to_wait.append((dag_id, run_id))

            if self.wait_for_completion and not failed:
                wait_result = self._wait_for_dag_runs_with_retry(
                    [(dag_id, run_id)],
                    conf,
                    normalized_store,
                )
                wait_results.append(wait_result)
                self._apply_wait_failures(failed, wait_result)
                if failed:
                    break

        combined_wait_result: Optional[Dict[str, Any]] = None
        if wait_results:
            succeeded_runs: List[Dict[str, str]] = []
            failed_runs: List[Dict[str, str]] = []
            for wait_result in wait_results:
                succeeded_runs.extend(wait_result.get("succeeded_runs", []))
                failed_runs.extend(wait_result.get("failed_runs", []))
            combined_wait_result = {
                "succeeded_runs": succeeded_runs,
                "failed_runs": failed_runs,
                "all_success": len(failed) == 0,
                "mode": _EXECUTION_MODE_SERIAL,
            }

        return {
            "triggered_dags": triggered,
            "skipped_dags": skipped,
            "failed_dags": failed,
            "monitored_runs": [
                {"dag_id": dag_id, "run_id": run_id}
                for dag_id, run_id in runs_to_wait
            ],
            "wait_result": combined_wait_result,
        }

    def _wait_for_dag_runs(
        self,
        dag_runs: List[Tuple[str, str]],
    ) -> Dict[str, Any]:
        """Poll downstream DagRuns until all reach a terminal state."""
        pending = {(dag_id, run_id) for dag_id, run_id in dag_runs}
        succeeded: List[Dict[str, str]] = []
        failed: List[Dict[str, str]] = []

        while pending:
            next_pending: Set[Tuple[str, str]] = set()

            for dag_id, run_id in pending:
                state = self._get_existing_run_state(dag_id, run_id)

                if state in _SUCCESS_DAG_RUN_STATES:
                    succeeded.append(
                        {"dag_id": dag_id, "run_id": run_id, "state": state}
                    )
                    continue

                if state in _FAILED_DAG_RUN_STATES:
                    failed.append(
                        {"dag_id": dag_id, "run_id": run_id, "state": state}
                    )
                    continue

                if state in _ACTIVE_DAG_RUN_STATES or state is None:
                    next_pending.add((dag_id, run_id))
                    continue

                failed.append(
                    {
                        "dag_id": dag_id,
                        "run_id": run_id,
                        "state": state or "unknown",
                    }
                )

            if not next_pending:
                break

            self.logger.info(
                "[DagSyncTrigger._wait_for_dag_runs] Waiting for %d downstream DagRun(s) to complete: %s",
                len(next_pending),
                sorted(next_pending),
            )
            pending = next_pending
            time.sleep(self.poke_interval)

        return {
            "succeeded_runs": succeeded,
            "failed_runs": failed,
            "all_success": len(failed) == 0,
        }

    def _wait_for_dag_runs_with_retry(
        self,
        dag_runs: List[Tuple[str, str]],
        conf: Dict[str, str],
        normalized_store: str,
    ) -> Dict[str, Any]:
        """Wait for DagRuns and optionally clear+retrigger failed runs."""
        wait_result = self._wait_for_dag_runs(dag_runs)

        for retry_attempt in range(1, self.failed_dag_run_retries + 1):
            failed_runs = wait_result.get("failed_runs", [])
            if not failed_runs:
                break

            self.logger.info(
                "[DagSyncTrigger._wait_for_dag_runs_with_retry] Auto-retry %d/%d "
                "for %d failed DagRun(s): %s",
                retry_attempt,
                self.failed_dag_run_retries,
                len(failed_runs),
                failed_runs,
            )
            time.sleep(self.failed_dag_run_retry_delay)

            runs_to_retry: List[Tuple[str, str]] = []
            for run in failed_runs:
                dag_id = run["dag_id"]
                run_id = run["run_id"]
                if not self._use_task_sdk():
                    self._clear_dag_run_orm(dag_id, run_id)
                logical_date = self.logical_date_for_store(normalized_store, dag_id)
                self._trigger_dag(
                    dag_id,
                    run_id,
                    conf,
                    logical_date=logical_date,
                    reset_dag_run=True,
                )
                runs_to_retry.append((dag_id, run_id))

            retry_wait = self._wait_for_dag_runs(runs_to_retry)
            retried_keys = set(runs_to_retry)

            succeeded = [
                run
                for run in wait_result.get("succeeded_runs", [])
                if (run["dag_id"], run["run_id"]) not in retried_keys
            ]
            succeeded.extend(retry_wait.get("succeeded_runs", []))

            still_failed = [
                run
                for run in wait_result.get("failed_runs", [])
                if (run["dag_id"], run["run_id"]) not in retried_keys
            ]
            failed = still_failed + retry_wait.get("failed_runs", [])

            wait_result = {
                "succeeded_runs": succeeded,
                "failed_runs": failed,
                "all_success": len(failed) == 0,
            }

        return wait_result

    def _trigger_dag(
        self,
        dag_id: str,
        run_id: str,
        conf: Dict[str, str],
        *,
        logical_date: datetime,
        reset_dag_run: bool = False,
    ) -> None:
        if self._use_task_sdk():
            self._trigger_dag_sdk(
                dag_id,
                run_id,
                conf,
                logical_date=logical_date,
                reset_dag_run=reset_dag_run,
            )
            return
        self._trigger_dag_orm(
            dag_id,
            run_id,
            conf,
            logical_date=logical_date,
            reset_dag_run=reset_dag_run,
        )

    def _trigger_dag_sdk(
        self,
        dag_id: str,
        run_id: str,
        conf: Dict[str, str],
        *,
        logical_date: datetime,
        reset_dag_run: bool = False,
    ) -> None:
        from airflow.sdk.exceptions import ErrorType  # type: ignore
        from airflow.sdk.execution_time.comms import ErrorResponse, TriggerDagRun  # type: ignore
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS  # type: ignore

        comms_msg = SUPERVISOR_COMMS.send(
            TriggerDagRun(
                dag_id=dag_id,
                run_id=run_id,
                conf=conf,
                logical_date=logical_date,
                reset_dag_run=reset_dag_run,
            ),
        )
        if isinstance(comms_msg, ErrorResponse):
            if comms_msg.error == ErrorType.DAGRUN_ALREADY_EXISTS:
                existing_state = self._get_existing_run_state(dag_id, run_id)
                if existing_state in _ACTIVE_DAG_RUN_STATES:
                    self.logger.info(
                        "[DagSyncTrigger._trigger_dag_sdk] DagRun already active for dag=%s run_id=%s state=%s",
                        dag_id,
                        run_id,
                        existing_state,
                    )
                    return
                if not reset_dag_run:
                    self.logger.info(
                        "[DagSyncTrigger._trigger_dag_sdk] DagRun already exists for dag=%s run_id=%s state=%s; resetting",
                        dag_id,
                        run_id,
                        existing_state,
                    )
                    self._trigger_dag_sdk(
                        dag_id,
                        run_id,
                        conf,
                        logical_date=logical_date,
                        reset_dag_run=True,
                    )
                    return
                self.logger.warning(
                    "[DagSyncTrigger._trigger_dag_sdk] DagRun already exists for dag=%s run_id=%s after reset attempt",
                    dag_id,
                    run_id,
                )
                return
            raise RuntimeError(f"Failed to trigger {dag_id}: {comms_msg.error}")

    def _trigger_dag_orm(
        self,
        dag_id: str,
        run_id: str,
        conf: Dict[str, str],
        *,
        logical_date: datetime,
        reset_dag_run: bool = False,
    ) -> None:
        from airflow.api.common.trigger_dag import trigger_dag  # type: ignore
        from airflow.exceptions import DagRunAlreadyExists  # type: ignore

        try:
            trigger_dag(
                dag_id=dag_id,
                run_id=run_id,
                conf=conf,
                logical_date=logical_date,
                replace_microseconds=False,
            )
        except DagRunAlreadyExists:
            existing_state = self._get_existing_run_state(dag_id, run_id)
            if existing_state in _ACTIVE_DAG_RUN_STATES:
                self.logger.info(
                    "[DagSyncTrigger._trigger_dag_orm] DagRun already active for dag=%s run_id=%s state=%s",
                    dag_id,
                    run_id,
                    existing_state,
                )
                return
            if reset_dag_run:
                self.logger.warning(
                    "[DagSyncTrigger._trigger_dag_orm] DagRun already exists for dag=%s run_id=%s after reset attempt",
                    dag_id,
                    run_id,
                )
                return
            self._clear_dag_run_orm(dag_id, run_id)
            trigger_dag(
                dag_id=dag_id,
                run_id=run_id,
                conf=conf,
                logical_date=logical_date,
                replace_microseconds=False,
            )

    @staticmethod
    def _build_dag_statuses(
        trigger_result: Dict[str, Any],
        store_number: str,
        parent_run_id: str,
    ) -> List[Dict[str, Any]]:
        """Merge trigger outcomes and wait results into per-DAG status records."""
        triggered_set = set(trigger_result.get("triggered_dags", []))
        skipped_set = set(trigger_result.get("skipped_dags", []))
        failed_list = trigger_result.get("failed_dags", [])
        failed_map = {item["dag_id"]: item.get("error") for item in failed_list}

        run_id_map = {
            run["dag_id"]: run["run_id"]
            for run in trigger_result.get("monitored_runs", [])
        }

        wait_result = trigger_result.get("wait_result")
        succeeded_map: Dict[str, Dict[str, str]] = {}
        wait_failed_map: Dict[str, Dict[str, str]] = {}
        if wait_result:
            for run in wait_result.get("succeeded_runs", []):
                succeeded_map[run["dag_id"]] = run
            for run in wait_result.get("failed_runs", []):
                wait_failed_map[run["dag_id"]] = run

        all_dag_ids = sorted(
            triggered_set
            | skipped_set
            | set(failed_map.keys())
            | set(run_id_map.keys())
        )

        dag_statuses: List[Dict[str, Any]] = []
        for dag_id in all_dag_ids:
            if dag_id in skipped_set:
                action = "skipped"
            elif dag_id in triggered_set:
                action = "triggered"
            else:
                action = "failed"

            run_id = run_id_map.get(dag_id) or DagSyncTrigger.build_run_id(
                store_number, dag_id, parent_run_id
            )

            error: Optional[str] = None
            if action == "failed":
                status = "failed"
                error = failed_map.get(dag_id)
            elif wait_result is not None:
                if dag_id in succeeded_map:
                    status = "success"
                    run_id = succeeded_map[dag_id].get("run_id", run_id)
                elif dag_id in wait_failed_map:
                    status = "failed"
                    wait_run = wait_failed_map[dag_id]
                    run_id = wait_run.get("run_id", run_id)
                    error = failed_map.get(dag_id) or (
                        f"DagRun finished with state={wait_run['state']}"
                    )
                elif dag_id in failed_map:
                    status = "failed"
                    error = failed_map[dag_id]
                else:
                    status = action
            else:
                status = action

            entry: Dict[str, Any] = {
                "dag_id": dag_id,
                "action": action,
                "status": status,
                "run_id": run_id,
            }
            if error:
                entry["error"] = error
            dag_statuses.append(entry)

        return dag_statuses

    @staticmethod
    def log_trigger_metrics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Log structured per-store and aggregated sync trigger metrics."""
        logger = logging.getLogger("DagSyncTrigger")
        logger.info("----- Store Sync Metrics -----")

        totals = {
            "triggered": 0,
            "skipped": 0,
            "succeeded": 0,
            "failed": 0,
            "duration": 0.0,
        }
        all_dag_statuses: List[Dict[str, Any]] = []

        for result in results:
            store = result["store_number"]
            triggered = result.get("triggered_count", 0)
            skipped = result.get("skipped_count", 0)
            succeeded = result.get("succeeded_count", 0)
            failed = result.get("failed_count", 0)
            duration = result.get("duration_seconds", 0.0)

            logger.info(
                "Store %s -> triggered=%d, skipped=%d, succeeded=%d, failed=%d, duration=%.2fs",
                store,
                triggered,
                skipped,
                succeeded,
                failed,
                duration,
            )

            for dag_status in result.get("dag_statuses", []):
                line = (
                    f"Store {store} -> {dag_status['dag_id']} -> "
                    f"status={dag_status['status']}, action={dag_status['action']}, "
                    f"run_id={dag_status['run_id']}"
                )
                if dag_status.get("error"):
                    line += f", error={dag_status['error']}"
                logger.info(line)
                all_dag_statuses.append({"store_number": store, **dag_status})

            totals["triggered"] += triggered
            totals["skipped"] += skipped
            totals["succeeded"] += succeeded
            totals["failed"] += failed
            totals["duration"] += duration

        logger.info("----- Aggregated Sync Metrics -----")
        logger.info(
            "TOTAL -> triggered=%d, skipped=%d, succeeded=%d, failed=%d, duration=%.2fs",
            totals["triggered"],
            totals["skipped"],
            totals["succeeded"],
            totals["failed"],
            totals["duration"],
        )
        logger.info("-----------------------------------")

        return {
            "stores": len(results),
            "dag_statuses": all_dag_statuses,
            "totals": totals,
        }

    def _clear_dag_run_orm(self, dag_id: str, run_id: str) -> None:
        """Remove a terminal DagRun so it can be re-triggered with the same run_id."""
        try:
            from airflow.models import DagRun  # type: ignore
            from airflow.utils.session import create_session  # type: ignore

            with create_session() as session:
                dag_run = (
                    session.query(DagRun)
                    .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
                    .one_or_none()
                )
                if dag_run:
                    session.delete(dag_run)
        except Exception as exc:
            self.logger.warning(
                "[DagSyncTrigger._clear_dag_run_orm] Could not delete DagRun for %s/%s: %s",
                dag_id,
                run_id,
                exc,
            )

    def trigger_store(self, store_number: Any) -> Dict[str, Any]:
        """
        Trigger all sync orchestrators for a single store.

        Skips orchestrators that already have a queued or running run with the
        same deterministic run_id. Execution order depends on execution_mode.
        """
        normalized_store = str(store_number).strip()
        conf = self.build_conf(normalized_store)
        started_at = time.perf_counter()

        if self.execution_mode == _EXECUTION_MODE_PARALLEL:
            result = self._trigger_store_parallel(normalized_store, conf)
        else:
            result = self._trigger_store_serial(normalized_store, conf)

        duration_seconds = time.perf_counter() - started_at
        failed = result["failed_dags"]
        dag_statuses = self._build_dag_statuses(
            result, normalized_store, self.parent_run_id
        )

        return {
            "store_number": normalized_store,
            "execution_mode": self.execution_mode,
            "triggered_dags": result["triggered_dags"],
            "skipped_dags": result["skipped_dags"],
            "failed_dags": failed,
            "monitored_runs": result["monitored_runs"],
            "wait_result": result["wait_result"],
            "success": len(failed) == 0,
            "duration_seconds": duration_seconds,
            "triggered_count": len(result["triggered_dags"]),
            "skipped_count": len(result["skipped_dags"]),
            "succeeded_count": sum(
                1 for status in dag_statuses if status["status"] == "success"
            ),
            "failed_count": sum(
                1 for status in dag_statuses if status["status"] == "failed"
            ),
            "dag_statuses": dag_statuses,
        }

    def trigger_stores(
        self,
        store_numbers: Set[str],
        emit_report: bool = True,
    ) -> List[Dict[str, Any]]:
        """Trigger sync orchestrators for a deduplicated set of store numbers."""
        results: List[Dict[str, Any]] = []
        for store_number in sorted(store_numbers):
            results.append(self.trigger_store(store_number))
        if emit_report and results:
            self.log_trigger_metrics(results)
        return results

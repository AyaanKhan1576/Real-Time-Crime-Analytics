"""
Module: progress_logging.py
Description: Lightweight periodic progress logging for Spark batch runs.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def configure_run_logging(run_id: str, configured_log_dir: str | None = None) -> Path:
    """
    Attach a per-run file handler to the root logger.

    Parameters
    ----------
    run_id : str
        Spark batch run identifier.

    Returns
    -------
    Path
        Log file path inside the runtime environment.
    """
    log_dir = Path(configured_log_dir or "/app/logs/spark")
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        log_dir = Path("logs/spark")
        log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / f"{run_id}.log"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    resolved_path = str(log_path.resolve())
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == resolved_path:
            return log_path

    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(file_handler)
    return log_path


class SparkProgressTracker:
    """
    Logs step-level and periodic heartbeat progress for long Spark runs.
    """

    def __init__(
        self,
        run_id: str,
        total_steps: int,
        logger: logging.Logger,
        interval_seconds: float | None = None,
    ) -> None:
        """
        Create a progress tracker.

        Parameters
        ----------
        run_id : str
            Spark batch run identifier.
        total_steps : int
            Number of planned driver-level stages.
        logger : logging.Logger
            Logger used for progress messages.
        interval_seconds : float | None
            Optional heartbeat interval. Defaults to 30 seconds.
        """
        self.run_id = run_id
        self.total_steps = total_steps
        self.logger = logger
        self.current_step = 0
        self.interval_seconds = interval_seconds or 30.0

    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        """
        Log progress for a single named stage.

        Parameters
        ----------
        name : str
            Stage name.
        """
        self.current_step += 1
        step_number = self.current_step
        completed_before = step_number - 1
        remaining_after = max(self.total_steps - step_number, 0)
        started_at = time.monotonic()
        stop_event = threading.Event()

        self.logger.info(
            "PROGRESS run_id=%s step=%s/%s START %s | completed=%s remaining_after_this=%s",
            self.run_id,
            step_number,
            self.total_steps,
            name,
            completed_before,
            remaining_after,
        )

        def heartbeat() -> None:
            while not stop_event.wait(self.interval_seconds):
                elapsed_seconds = time.monotonic() - started_at
                self.logger.info(
                    "PROGRESS run_id=%s step=%s/%s RUNNING %s | elapsed=%.1fs completed=%s remaining_after_this=%s",
                    self.run_id,
                    step_number,
                    self.total_steps,
                    name,
                    elapsed_seconds,
                    completed_before,
                    remaining_after,
                )

        thread: threading.Thread | None = None
        if self.interval_seconds > 0:
            thread = threading.Thread(target=heartbeat, daemon=True)
            thread.start()

        try:
            yield
        except Exception:
            elapsed_seconds = time.monotonic() - started_at
            self.logger.exception(
                "PROGRESS run_id=%s step=%s/%s FAILED %s | elapsed=%.1fs",
                self.run_id,
                step_number,
                self.total_steps,
                name,
                elapsed_seconds,
            )
            raise
        else:
            elapsed_seconds = time.monotonic() - started_at
            self.logger.info(
                "PROGRESS run_id=%s step=%s/%s DONE %s | elapsed=%.1fs completed=%s remaining=%s",
                self.run_id,
                step_number,
                self.total_steps,
                name,
                elapsed_seconds,
                step_number,
                remaining_after,
            )
        finally:
            stop_event.set()
            if thread is not None:
                thread.join(timeout=1)

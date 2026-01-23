from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .auth import OAuthTokenProvider
from .config import AppConfig
from .errors import QueryError
from .logging_utils import log_extra


class DatabricksJobsClient:
    def __init__(self, config: AppConfig, token_provider: OAuthTokenProvider) -> None:
        self._config = config
        self._token_provider = token_provider
        self._log = logging.getLogger(__name__)
        self._base_url = f"https://{self._config.warehouse.host}/api/2.1/jobs"

    def _make_request(
        self, method: str, endpoint: str, data: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Make authenticated requests to Databricks Jobs API."""
        access_token = self._token_provider.get_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        url = f"{self._base_url}/{endpoint}" if endpoint else self._base_url
        response = requests.request(
            method=method,
            url=url,
            json=data,
            headers=headers,
            timeout=30,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self._log.error(f"Jobs API error: {response.text}")
            raise QueryError(f"Jobs API request failed: {e}") from e

        return response.json() if response.text else {}

    def submit_python_job(
        self,
        job_name: str,
        python_code: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Submit a Python script for one-time execution using Submit API.

        Args:
            job_name: Name of the run
            python_code: Python script content
            request_id: Request tracking ID

        Returns:
            Submit response with run_id
        """
        payload = {
            "run_name": job_name,
            "timeout_seconds": 3600,
            "tasks": [
                {
                    "task_key": "python_task",
                    "python_script_task": {"script_content": python_code},
                    "new_cluster": {
                        "spark_version": "15.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 1,
                        "aws_attributes": {"availability": "SPOT_WITH_FALLBACK"},
                    },
                }
            ],
            "tags": {"execution_type": "python_script"},
        }

        response = self._make_request("POST", "runs/submit", payload)
        run_id = response.get("run_id")

        self._log.info(
            "Python job submitted",
            extra=log_extra(request_id=request_id, run_id=run_id),
        )
        return response

    def submit_notebook_job(
        self,
        job_name: str,
        notebook_path: str,
        parameters: dict[str, str] | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Submit a notebook for one-time execution using Submit API.

        Args:
            job_name: Name of the run
            notebook_path: Path to the notebook in Databricks workspace
            parameters: Optional notebook parameters as key-value pairs
            request_id: Request tracking ID

        Returns:
            Submit response with run_id
        """
        task_config: dict[str, Any] = {
            "notebook_task": {
                "notebook_path": notebook_path,
            }
        }
        if parameters:
            task_config["notebook_task"]["base_parameters"] = parameters

        payload = {
            "run_name": job_name,
            "timeout_seconds": 3600,
            "tasks": [
                {
                    "task_key": "notebook_task",
                    **task_config,
                    "new_cluster": {
                        "spark_version": "15.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 1,
                        "aws_attributes": {"availability": "SPOT_WITH_FALLBACK"},
                    },
                }
            ],
            "tags": {"execution_type": "notebook"},
        }

        response = self._make_request("POST", "runs/submit", payload)
        run_id = response.get("run_id")

        self._log.info(
            "Notebook job submitted",
            extra=log_extra(request_id=request_id, run_id=run_id),
        )
        return response

    def get_job_status(
        self,
        run_id: int,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Get the status of a job run.

        Args:
            run_id: The run ID
            request_id: Request tracking ID

        Returns:
            Job run details including state and result_state
        """
        response = self._make_request("GET", f"runs/get?run_id={run_id}")

        self._log.info(
            "Job status retrieved",
            extra=log_extra(
                request_id=request_id,
                run_id=run_id,
                state=response.get("state"),
            ),
        )
        return response

    def wait_for_job_completion(
        self,
        run_id: int,
        timeout_seconds: int = 3600,
        poll_interval: int = 5,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Poll job status until completion.

        Args:
            run_id: The run ID
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks
            request_id: Request tracking ID

        Returns:
            Final job run details

        Raises:
            QueryError: If job fails or times out
        """
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise QueryError(f"Run {run_id} timed out after {timeout_seconds}s")

            response = self.get_job_status(run_id, request_id)
            state = response.get("state", {}).get("life_cycle_state")

            if state == "TERMINATED":
                result_state = response.get("state", {}).get("result_state")
                if result_state == "SUCCESS":
                    self._log.info(
                        "Job completed successfully",
                        extra=log_extra(request_id=request_id, run_id=run_id),
                    )
                    return response
                else:
                    error_msg = response.get("state", {}).get(
                        "state_message", "Job failed"
                    )
                    raise QueryError(
                        f"Job failed with state {result_state}: {error_msg}"
                    )

            if state == "INTERNAL_ERROR":
                raise QueryError("Job encountered an internal error")

            time.sleep(poll_interval)

    def get_job_run_output(
        self,
        run_id: int,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve output and logs from a completed job run.

        Args:
            run_id: The run ID
            request_id: Request tracking ID

        Returns:
            Job run output including logs and any task outputs
        """
        response = self._make_request("GET", f"runs/get-output?run_id={run_id}")

        self._log.info(
            "Job output retrieved",
            extra=log_extra(request_id=request_id, run_id=run_id),
        )
        return response

    def execute_python_code(
        self,
        code: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute Python code and wait for completion.

        Args:
            code: Python code to execute
            request_id: Request tracking ID

        Returns:
            Dictionary with run_id and output
        """
        submit_response = self.submit_python_job(
            job_name=f"python_execution_{int(time.time())}",
            python_code=code,
            request_id=request_id,
        )

        run_id = submit_response.get("run_id")

        if not run_id:
            raise QueryError("Failed to retrieve run_id from response")

        completion_response = self.wait_for_job_completion(
            run_id, request_id=request_id
        )

        output_response = self.get_job_run_output(run_id, request_id=request_id)

        return {
            "run_id": run_id,
            "status": completion_response.get("state", {}).get("result_state"),
            "output": output_response.get("output", {}),
        }

    def cancel_run(
        self,
        run_id: int,
        request_id: str | None = None,
    ) -> None:
        """
        Cancel an active job run.

        Args:
            run_id: The run ID to cancel
            request_id: Request tracking ID
        """
        self._make_request("POST", "runs/cancel", {"run_id": run_id})

        self._log.info(
            "Job run cancelled",
            extra=log_extra(request_id=request_id, run_id=run_id),
        )

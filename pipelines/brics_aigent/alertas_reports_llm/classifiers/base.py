# -*- coding: utf-8 -*-
"""
Base Classifier Module

Contains the abstract base class for all BRICS LLM classifiers.
Provides common functionality and interface definition.
"""

import logging
import re
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict

import dspy
import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.environment_vars import getenv_or_action


def safe_log(message: str, level: str = "info"):
    """
    Thread-safe logging function that works in both main thread and worker threads.

    Args:
        message: Message to log
        level: Log level (info, warning, error, debug)
    """
    try:
        # Try to use Prefect's logger first
        log(message, level=level)
    except (AttributeError, Exception):
        # Fallback to standard logging if Prefect context is not available
        logger = logging.getLogger(__name__)
        level_mapping = {
            "debug": logger.debug,
            "info": logger.info,
            "warning": logger.warning,
            "error": logger.error,
            "critical": logger.critical,
        }
        log_func = level_mapping.get(level.lower(), logger.info)
        log_func(message)


class BaseClassifier(ABC):
    """
    Abstract base class for all BRICS LLM classifiers.

    Provides common functionality including:
    - Model configuration
    - Token logging
    - DataFrame processing with optional threading
    - Error handling
    """

    def __init__(
        self,
        api_key: str = None,
        model_name: str = "gemini/gemini-2.5-flash",
        temperature: float = 0.5,
        max_tokens: int = 1024,
        use_existing_dspy_config: bool = True,
    ):
        """
        Initialize the base classifier.

        Args:
            api_key: API key for the LLM service
            model_name: Name of the LLM model to use
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens to generate
            use_existing_dspy_config: Whether to use existing DSPy configuration
        """
        self.api_key = api_key
        self.model_name = model_name
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.token_logs = []
        self.use_existing_dspy_config = use_existing_dspy_config

        if not use_existing_dspy_config:
            self._configure_model()
        else:
            self._use_existing_dspy_config()

        self._setup_classifier()

    def _configure_model(self):
        """Configure the DSPy language model (legacy method)."""
        if not self.api_key:
            self.api_key = getenv_or_action("GOOGLE_API_KEY", action="raise")

        # Create the LM instance
        self.lm = dspy.LM(
            self.model_name,
            api_key=self.api_key,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )

        # Configure DSPy
        dspy.configure(lm=self.lm, track_usage=True)

        safe_log(
            f"Configured LLM: {self.model_name} (temp={self.temperature}, max_tokens={self.max_tokens})"
        )

    def _use_existing_dspy_config(self):
        """Use existing DSPy configuration."""
        if not hasattr(dspy.settings, "lm") or dspy.settings.lm is None:
            raise RuntimeError("DSPy not configured. Please run task_configure_dspy first.")

        safe_log(f"Using existing DSPy configuration for {self.__class__.__name__}")

    @abstractmethod
    def _setup_classifier(self):
        """Setup the specific classifier implementation. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def classify_single(self, descricao: str) -> Dict[str, Any]:
        """
        Classify a single description and return results.
        Must be implemented by subclasses.

        Args:
            descricao: Text description to classify

        Returns:
            Dictionary with classification results
        """
        pass

    @abstractmethod
    def _get_error_result(self, error_msg: str) -> Dict[str, Any]:
        """
        Return appropriate error result for this classifier type.
        Must be implemented by subclasses.

        Args:
            error_msg: Error message

        Returns:
            Dictionary with error result structure
        """
        pass

    def _extract_final_description(self, history_entry):
        """Extract the final description from LLM message history."""
        messages = history_entry.get("messages", [])
        if not messages:
            return None

        # Get the last user message
        last_user_msg = next(
            (m["content"] for m in reversed(messages) if m.get("role") == "user"), None
        )

        if not last_user_msg:
            return None

        # Try to extract description from specific format
        match = re.search(r"\[\[ ## descricao ## \]\]\n(.*?)$", last_user_msg, re.DOTALL)
        return match.group(1).strip() if match else last_user_msg.strip()

    def _classify_single_with_logging(self, row):
        """
        Classify a single DataFrame row and record comprehensive logs.

        Args:
            row: DataFrame row with 'descricao' column

        Returns:
            Dictionary with classification results and metadata
        """
        try:
            # Perform classification
            result = self.classify_single(row["descricao"])

            # Extract token usage from LLM history
            if (
                hasattr(dspy.settings, "lm")
                and dspy.settings.lm
                and hasattr(dspy.settings.lm, "history")
                and dspy.settings.lm.history
            ):
                h = dspy.settings.lm.history[-1]
                h["etapa"] = "classificação"

                # Extract description for logging
                descricao = self._extract_final_description(h)

                # Create comprehensive log entry
                log_entry = {
                    "descricao": descricao,
                    "total_tokens": h.get("usage", {}).get("total_tokens", 0),
                    "prompt_tokens": h.get("usage", {}).get("prompt_tokens", 0),
                    "completion_tokens": h.get("usage", {}).get("completion_tokens", 0),
                    "custo": h.get("cost", 0.0),
                    "model": self.model_name,
                    "temperature": self.temperature,
                }

                # Merge classification result with log entry
                log_entry.update(result)
                self.token_logs.append(log_entry)

            return result

        except Exception as e:
            error_msg = (
                f"Classification error for description '{row['descricao'][:50]}...': {str(e)}"
            )
            safe_log(error_msg, level="error")
            return self._get_error_result(str(e))

    def classify_dataframe(
        self,
        df: pd.DataFrame,
        use_threading: bool = False,
        max_workers: int = 10,
        progress_interval: int = 50,
    ) -> pd.DataFrame:
        """
        Classify a complete DataFrame with optimized performance.

        Args:
            df: DataFrame with 'descricao' column
            use_threading: Whether to use threading for parallel processing
            max_workers: Number of worker threads if using threading
            progress_interval: How often to log progress (every N items)

        Returns:
            DataFrame with classification results
        """
        if df.empty:
            safe_log("Empty DataFrame provided for classification", level="warning")
            return pd.DataFrame()

        safe_log(f"Starting classification of {len(df)} records using {self.__class__.__name__}")
        safe_log(f"Settings: threading={use_threading}, max_workers={max_workers}")

        # Clear previous logs
        self.token_logs = []

        # Choose processing method based on threading preference
        if use_threading and len(df) > 10:  # Only use threading for larger datasets
            result_df = self._classify_with_threading(df, max_workers, progress_interval)
        else:
            result_df = self._classify_sequential(df, progress_interval)

        # Log final statistics
        total_tokens = sum(log.get("total_tokens", 0) for log in self.token_logs)
        total_cost = sum(log.get("custo", 0.0) for log in self.token_logs)

        safe_log(f"Classification completed: {len(result_df)} records processed")
        safe_log(f"Total tokens used: {total_tokens:,}, Estimated cost: ${total_cost:.6f}")

        return result_df

    def _classify_sequential(self, df: pd.DataFrame, progress_interval: int) -> pd.DataFrame:
        """Sequential classification with progress logging."""
        results = []

        for i, (_, row) in enumerate(df.iterrows()):
            if i % progress_interval == 0 and i > 0:
                safe_log(f"Processing item {i}/{len(df)} ({i/len(df)*100:.1f}%)")

            results.append(self._classify_single_with_logging(row))

        return pd.DataFrame(results)

    def _classify_with_threading(
        self, df: pd.DataFrame, max_workers: int, progress_interval: int
    ) -> pd.DataFrame:
        """Threaded classification with progress tracking."""
        results = [None] * len(df)

        def classify_with_index(args):
            index, row = args
            return index, self._classify_single_with_logging(row)

        # Create indexed data for maintaining order
        indexed_data = list(enumerate(df.iterrows()))
        indexed_rows = [(i, row) for i, (_, row) in indexed_data]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(classify_with_index, row_data): i
                for i, row_data in enumerate(indexed_rows)
            }

            # Process completed tasks
            completed_count = 0
            for future in as_completed(future_to_index):
                index, result = future.result()
                results[index] = result
                completed_count += 1

                # Log progress
                if completed_count % progress_interval == 0:
                    progress = completed_count / len(df) * 100
                    safe_log(
                        f"Completed {completed_count}/{len(df)} classifications ({progress:.1f}%)"
                    )

        return pd.DataFrame(results)

    def get_token_logs_df(self) -> pd.DataFrame:
        """
        Get token usage logs as a DataFrame.

        Returns:
            DataFrame with token usage information
        """
        return pd.DataFrame(self.token_logs)

    def get_usage_summary(self) -> Dict[str, Any]:
        """
        Get summary of token usage and costs.

        Returns:
            Dictionary with usage statistics
        """
        if not self.token_logs:
            return {"total_tokens": 0, "total_cost": 0.0, "total_requests": 0}

        total_tokens = sum(log.get("total_tokens", 0) for log in self.token_logs)
        total_cost = sum(log.get("custo", 0.0) for log in self.token_logs)
        total_requests = len(self.token_logs)

        return {
            "total_tokens": total_tokens,
            "total_cost": total_cost,
            "total_requests": total_requests,
            "avg_tokens_per_request": total_tokens / total_requests if total_requests > 0 else 0,
            "model": self.model_name,
            "temperature": self.temperature,
        }

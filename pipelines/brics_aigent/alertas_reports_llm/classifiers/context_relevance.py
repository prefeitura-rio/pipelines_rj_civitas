# -*- coding: utf-8 -*-
"""
Context Relevance classifier for BRICS alerts.
"""

from typing import Any, Dict

import dspy
import pandas as pd

from .base import BaseClassifier, safe_log


class ContextRelevanceSignature(dspy.Signature):
    """Signature for analyzing event relevance to specific contexts."""

    prompt_llm: str = dspy.InputField(desc="Prompt estruturado com ocorrência + contexto")
    is_relevant: bool = dspy.OutputField(desc="True se o evento for relevante para o contexto")
    relevance_reasoning: str = dspy.OutputField(desc="Justificativa objetiva e curta")


class ContextRelevanceAnalyzer(dspy.Module):
    """DSPy module for analyzing context relevance of incident reports."""

    def __init__(self):
        super().__init__()
        # Use Predict instead of ChainOfThought to avoid field conflicts
        self.analyze = dspy.Predict(ContextRelevanceSignature)

    def forward(self, prompt_llm: str) -> dspy.Prediction:
        return self.analyze(prompt_llm=prompt_llm)


class ContextRelevanceClassifier(BaseClassifier):
    """
    Context relevance classifier for analyzing event relevance to specific contexts.

    This classifier analyzes whether an incident is relevant to a specific context
    based on geographical proximity and content analysis.
    """

    def __init__(
        self,
        model_name: str,
        temperature: float,
        max_tokens: int,
        use_existing_dspy_config: bool = True,
        api_key: str | None = None,
    ):
        """
        Initialize the context relevance classifier.

        Args:
            api_key: API key for the LLM
            model_name: Name of the LLM model
            temperature: Temperature for generation (lower for more consistent analysis)
            max_tokens: Maximum tokens for response
            use_existing_dspy_config: Whether to use existing DSPy configuration
        """
        super().__init__(api_key, model_name, temperature, max_tokens, use_existing_dspy_config)

    def _setup_classifier(self):
        """Setup the context relevance analyzer instance."""
        self.analyzer = ContextRelevanceAnalyzer()

    def classify_single(self, prompt_llm: str, **kwargs) -> Dict[str, Any]:
        """
        Analyze relevance of an event to a context based on the structured prompt.

        Args:
            prompt_llm: Structured prompt with event and context information
            **kwargs: Additional fields (id_report, contexto_id)

        Returns:
            Dictionary with relevance analysis results
        """
        try:
            # Perform relevance analysis
            result = self.analyzer(prompt_llm=prompt_llm)

            return {
                "is_relevant": getattr(result, "is_relevant", False),
                "relevance_reasoning": getattr(result, "relevance_reasoning", ""),
                "analysis_type": "context_relevance",
            }

        except Exception as e:
            # Use try-catch for logging to avoid Prefect context issues
            try:
                safe_log(f"Error in context relevance analysis: {str(e)}", level="error")
            except Exception as e:
                print(f"Error in context relevance analysis: {str(e)}")
            return self._get_error_result(str(e))

    def analyze_relevance_dataframe(
        self,
        df: pd.DataFrame,
        use_threading: bool = False,
        max_workers: int = 20,
        progress_interval: int = 50,
    ) -> pd.DataFrame:
        """
        Analyze context relevance for a complete DataFrame of prompts.

        Args:
            df: DataFrame with columns (id_report, contexto_id, prompt_llm)
            use_threading: Whether to use threading for parallel processing
            max_workers: Number of worker threads if using threading
            progress_interval: How often to log progress

        Returns:
            DataFrame with relevance analysis results
        """
        if df.empty:
            safe_log("Empty DataFrame provided for context relevance analysis", level="warning")
            return pd.DataFrame()

        # Verify required columns
        required_cols = ["prompt_llm"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        log_msg = (
            f"Starting context relevance analysis for {len(df)} prompts using {self.__class__.__name__}\n"
            f"Settings: threading={use_threading}, max_workers={max_workers}"
        )
        safe_log(log_msg)

        # Clear previous logs
        self.token_logs = []

        # Override the analysis method to handle prompt structure
        def analyze_single_with_logging(row):
            try:
                # Perform relevance analysis
                result = self.classify_single(
                    row["prompt_llm"],
                    id_report=row.get("id_report", ""),
                    contexto_id=row.get("contexto_id", ""),
                )

                # Extract token usage from LLM history
                if dspy.settings.lm.history:
                    h = dspy.settings.lm.history[-1]
                    h["etapa"] = "relevância"

                    # Extract the actual prompt for logging
                    prompt = self._extract_prompt_relevancia(h)

                    # Create comprehensive log entry
                    log_entry = {
                        "id_report": row.get("id_report", ""),
                        "contexto_id": row.get("contexto_id", ""),
                        "prompt_llm": prompt,
                        "total_tokens": h.get("usage", {}).get("total_tokens", 0),
                        "prompt_tokens": h.get("usage", {}).get("prompt_tokens", 0),
                        "completion_tokens": h.get("usage", {}).get("completion_tokens", 0),
                        "custo": h.get("cost", 0.0),
                        "model": self.model_name,
                        "temperature": self.temperature,
                    }

                    # Merge analysis result with log entry
                    log_entry.update(result)
                    self.token_logs.append(log_entry)

                # Add context information to result
                result["id_report"] = row.get("id_report", "")
                result["contexto_id"] = row.get("contexto_id", "")

                return result

            except Exception as e:
                error_result = self._get_error_result(str(e))
                error_result["id_report"] = row.get("id_report", "")
                error_result["contexto_id"] = row.get("contexto_id", "")
                safe_log(f"Error in context relevance analysis: {str(e)}", level="error")
                return error_result

        # Choose processing method based on threading preference
        if use_threading and len(df) > 10:
            safe_log("Using threading for context relevance analysis", level="info")
            result_df = self._analyze_with_threading(
                df, max_workers, progress_interval, analyze_single_with_logging
            )
        else:
            safe_log("Using sequential context relevance analysis", level="info")
            result_df = self._analyze_sequential(df, progress_interval, analyze_single_with_logging)

        # Log final statistics
        total_tokens = sum(log.get("total_tokens", 0) for log in self.token_logs)
        total_cost = sum(log.get("custo", 0.0) for log in self.token_logs)
        relevant_count = result_df["is_relevant"].sum() if "is_relevant" in result_df.columns else 0

        log_msg = (
            f"Context relevance analysis completed: {len(result_df)} analyses processed\n"
            f"Relevant events found: {relevant_count}/{len(result_df)} ({relevant_count/len(result_df)*100:.1f}%)\n"
            f"Total tokens used: {total_tokens:,}, Estimated cost: ${total_cost:.6f}"
        )
        safe_log(log_msg)

        return result_df

    def _extract_prompt_relevancia(self, history_entry):
        """Extract the relevance prompt from LLM message history."""
        messages = history_entry.get("messages", [])
        if not messages:
            return None

        # Get the last user message
        last_user_msg = next(
            (m["content"] for m in reversed(messages) if m.get("role") == "user"), None
        )

        return last_user_msg.strip() if last_user_msg else None

    def _analyze_sequential(
        self, df: pd.DataFrame, progress_interval: int, analyze_func
    ) -> pd.DataFrame:
        """Sequential analysis with progress logging."""
        results = []

        for i, (_, row) in enumerate(df.iterrows()):
            if i % progress_interval == 0 and i > 0:
                safe_log(f"Processing analysis {i}/{len(df)} ({i/len(df)*100:.1f}%)")

            results.append(analyze_func(row))

        return pd.DataFrame(results)

    def _analyze_with_threading(
        self, df: pd.DataFrame, max_workers: int, progress_interval: int, analyze_func
    ) -> pd.DataFrame:
        """Threaded analysis with progress tracking."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = [None] * len(df)
        completed_count = 0

        def analyze_with_index(args):
            index, row = args
            return index, analyze_func(row)

        # Create indexed data for maintaining order
        indexed_data = list(enumerate(df.iterrows()))
        indexed_rows = [(i, row) for i, (_, row) in indexed_data]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(analyze_with_index, row_data): i
                for i, row_data in enumerate(indexed_rows)
            }

            # Process completed tasks
            for future in as_completed(future_to_index):
                index, result = future.result()
                results[index] = result
                completed_count += 1

                # Log progress
                if completed_count % progress_interval == 0:
                    progress = completed_count / len(df) * 100
                    safe_log(f"Completed {completed_count}/{len(df)} analyses ({progress:.1f}%)")

        return pd.DataFrame(results)

    def _get_error_result(self, error_msg: str) -> Dict[str, Any]:
        """
        Return error result for context relevance analysis.

        Args:
            error_msg: Error message

        Returns:
            Dictionary with error result structure
        """
        return {
            "is_relevant": False,
            "relevance_reasoning": f"Erro na análise: {error_msg}",
            "analysis_type": "context_relevance",
        }

    def get_relevance_stats(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics for context relevance analysis results.

        Args:
            results_df: DataFrame with analysis results

        Returns:
            Dictionary with relevance statistics
        """
        if results_df.empty:
            return {
                "total_analyses": 0,
                "relevant_count": 0,
                "relevance_rate": 0.0,
                "unique_events": 0,
                "unique_contexts": 0,
            }

        total_analyses = len(results_df)
        relevant_count = (
            results_df["is_relevant"].sum() if "is_relevant" in results_df.columns else 0
        )
        relevance_rate = relevant_count / total_analyses if total_analyses > 0 else 0.0

        unique_events = (
            results_df["id_report"].nunique() if "id_report" in results_df.columns else 0
        )
        unique_contexts = (
            results_df["contexto_id"].nunique() if "contexto_id" in results_df.columns else 0
        )

        return {
            "total_analyses": total_analyses,
            "relevant_count": relevant_count,
            "relevance_rate": relevance_rate,
            "unique_events": unique_events,
            "unique_contexts": unique_contexts,
        }

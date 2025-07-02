# -*- coding: utf-8 -*-
"""
Entity and Time Extraction classifier for BRICS alerts.
"""

from typing import Any, Dict, List

import dspy
import pandas as pd

from .base import BaseClassifier, safe_log


class EntityAndTimeExtractionSignature(dspy.Signature):
    """Signature for entity and time extraction from incident reports."""

    # Entradas
    data_report: str = dspy.InputField(desc="Data e hora do registro da denúncia")
    categoria: str = dspy.InputField(desc="Categoria da ocorrência")
    tipo_subtipo: str = dspy.InputField(desc="Tipo/Subtipo da ocorrência")
    orgaos: str = dspy.InputField(desc="Órgãos envolvidos")
    descricao: str = dspy.InputField(desc="Texto da denúncia")

    # Saídas - Usando strings simples em vez de listas para evitar problemas de parsing
    event_types: str = dspy.OutputField(
        desc="Tipos de eventos separados por vírgula (ex: tiroteio, assalto)"
    )
    locations: str = dspy.OutputField(desc="Locais mencionados separados por vírgula")
    people: str = dspy.OutputField(desc="Pessoas ou grupos citados separados por vírgula")
    event_time: str = dspy.OutputField(
        desc="Data e hora estimada do evento real (formato: YYYY-MM-DD HH:MM:SS)"
    )
    reasoning: str = dspy.OutputField(desc="Justificativa de como as datas foram inferidas")


class EntityAndTimeExtractor(dspy.Module):
    """DSPy module for extracting entities and time information from incident reports."""

    def __init__(self):
        super().__init__()
        self.extract = dspy.Predict(
            EntityAndTimeExtractionSignature,
            prompt="""
Você é um analista de segurança pública experiente.

Analise as seguintes informações da ocorrência:
- Data do relato: {data_report}
- Categoria: {categoria}
- Tipo/Subtipo: {tipo_subtipo}
- Órgãos envolvidos: {orgaos}
- Descrição: {descricao}

📌 Extraia as seguintes informações:

1. **Tipos de evento**: Identifique os tipos de eventos mencionados (ex: tiroteio, assalto, tráfico). Liste separados por vírgula.

2. **Locais**: Identifique locais específicos mencionados (ruas, bairros, pontos de referência). Liste separados por vírgula.

3. **Pessoas ou grupos**: Identifique pessoas, grupos ou organizações mencionadas. Liste separados por vírgula.

4. **Data e hora do evento**: Estime quando o evento descrito ocorreu, considerando:
   - Expressões temporais como "ontem", "hoje de manhã", "recentemente"
   - A data do relato como referência
   - Formato: YYYY-MM-DD HH:MM:SS

5. **Justificativa**: Explique brevemente como você inferiu a data e hora do evento.

**Importante**:
- Se não houver informação suficiente, use "não informado"
- Para data/hora, use a data do relato se não houver indicação temporal específica
- Seja preciso e objetivo nas extrações
""",
        )

    def forward(self, **kwargs) -> dspy.Prediction:
        return self.extract(**kwargs)


class EntityExtractionClassifier(BaseClassifier):
    """
    Entity and time extraction classifier for incident reports.

    This classifier extracts structured information from incident descriptions including:
    - Event types (e.g., shooting, robbery)
    - Locations mentioned
    - People or groups involved
    - Estimated event times
    - Reasoning for time inference
    """

    def __init__(
        self,
        api_key: str | None = None,
        model_name: str = "gemini/gemini-2.5-flash",
        temperature: float = 0.3,
        max_tokens: int = 1024,
        use_existing_dspy_config: bool = True,
    ):
        """
        Initialize the entity extraction classifier.

        Args:
            api_key: API key for the LLM
            model_name: Name of the LLM model
            temperature: Temperature for generation (lower for more consistent extractions)
            max_tokens: Maximum tokens for response
            use_existing_dspy_config: Whether to use existing DSPy configuration
        """
        super().__init__(api_key, model_name, temperature, max_tokens, use_existing_dspy_config)

    def _setup_classifier(self):
        """Setup the entity extractor instance."""
        self.extractor = EntityAndTimeExtractor()

    def _parse_comma_separated(self, text: str) -> List[str]:
        """Parse comma-separated text into a list, handling empty values."""
        if not text or text.strip().lower() in ["não informado", "n/a", ""]:
            return []

        items = [item.strip() for item in text.split(",") if item.strip()]
        return [item for item in items if item.lower() not in ["não informado", "n/a"]]

    def classify_single(self, description: str, **kwargs) -> Dict[str, Any]:
        """
        Extract entities from a single incident description.

        Args:
            description: The incident description text
            **kwargs: Additional fields (data_report, categoria, tipo_subtipo, orgaos)

        Returns:
            Dictionary with extracted entities and metadata
        """
        try:
            # Get additional context from kwargs
            data_report = kwargs.get("data_report", "")
            categoria = kwargs.get("categoria", "")
            tipo_subtipo = str(kwargs.get("tipo_subtipo", ""))
            orgaos = str(kwargs.get("orgaos", ""))

            # Perform extraction
            result = self.extractor(
                data_report=data_report,
                categoria=categoria,
                tipo_subtipo=tipo_subtipo,
                orgaos=orgaos,
                descricao=description,
            )

            # Safely extract and parse attributes
            event_types = self._parse_comma_separated(getattr(result, "event_types", ""))
            locations = self._parse_comma_separated(getattr(result, "locations", ""))
            people = self._parse_comma_separated(getattr(result, "people", ""))

            # Handle event_time as single string
            event_time_str = getattr(result, "event_time", "")
            event_time = (
                [event_time_str]
                if event_time_str and event_time_str.lower() != "não informado"
                else []
            )

            # Handle reasoning as single string
            reasoning_str = getattr(result, "reasoning", "")
            reasoning = (
                [reasoning_str]
                if reasoning_str and reasoning_str.lower() != "não informado"
                else []
            )

            return {
                "event_types": event_types,
                "locations": locations,
                "people": people,
                "event_time": event_time,
                "reasoning": reasoning,
                "extraction_type": "entity_extraction",
            }

        except Exception as e:
            safe_log(f"Error in entity extraction: {str(e)}", level="error")
            return self._get_error_result(str(e))

    def classify_dataframe(
        self,
        df: pd.DataFrame,
        use_threading: bool = False,
        max_workers: int = 10,
        progress_interval: int = 50,
    ) -> pd.DataFrame:
        """
        Extract entities from a complete DataFrame.

        Args:
            df: DataFrame with required columns (descricao, data_report, categoria, etc.)
            use_threading: Whether to use threading for parallel processing
            max_workers: Number of worker threads if using threading
            progress_interval: How often to log progress

        Returns:
            DataFrame with extraction results merged with original data
        """
        if df.empty:
            safe_log("Empty DataFrame provided for entity extraction", level="warning")
            return pd.DataFrame()

        # Verify required columns
        required_cols = ["descricao"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        safe_log(
            f"Starting entity extraction for {len(df)} records using {self.__class__.__name__}"
        )
        safe_log(f"Settings: threading={use_threading}, max_workers={max_workers}")

        # Clear previous logs
        self.token_logs = []

        # Override the _classify_single_with_logging method to handle additional fields
        def extract_single_with_logging(row):
            try:
                # Perform extraction with additional context
                result = self.classify_single(
                    row["descricao"],
                    data_report=row.get("data_report", ""),
                    categoria=row.get("categoria", ""),
                    tipo_subtipo=row.get("tipo_subtipo", ""),
                    orgaos=row.get("orgaos", ""),
                )

                # Extract token usage from LLM history
                if (
                    hasattr(dspy.settings, "lm")
                    and dspy.settings.lm
                    and hasattr(dspy.settings.lm, "history")
                    and dspy.settings.lm.history
                ):
                    h = dspy.settings.lm.history[-1]
                    h["etapa"] = "extração entidades"

                    # Create comprehensive log entry
                    log_entry = {
                        "descricao": row["descricao"],
                        "total_tokens": h.get("usage", {}).get("total_tokens", 0),
                        "prompt_tokens": h.get("usage", {}).get("prompt_tokens", 0),
                        "completion_tokens": h.get("usage", {}).get("completion_tokens", 0),
                        "custo": h.get("cost", 0.0),
                        "model": self.model_name,
                        "temperature": self.temperature,
                    }

                    # Merge extraction result with log entry
                    log_entry.update(result)
                    self.token_logs.append(log_entry)

                return result

            except Exception as e:
                safe_log(f"Error in entity extraction: {str(e)}", level="error")
                return self._get_error_result(str(e))

        # Choose processing method based on threading preference
        if use_threading and len(df) > 10:
            extraction_results_df = self._extract_with_threading(
                df, max_workers, progress_interval, extract_single_with_logging
            )
        else:
            extraction_results_df = self._extract_sequential(
                df, progress_interval, extract_single_with_logging
            )

        # Merge extraction results with original DataFrame
        # Reset index to ensure proper alignment
        df_reset = df.reset_index(drop=True)
        extraction_results_df_reset = extraction_results_df.reset_index(drop=True)

        # Concatenate original data with extraction results
        merged_df = pd.concat([df_reset, extraction_results_df_reset], axis=1)

        # Log final statistics
        total_tokens = sum(log.get("total_tokens", 0) for log in self.token_logs)
        total_cost = sum(log.get("custo", 0.0) for log in self.token_logs)

        safe_log(f"Entity extraction completed: {len(merged_df)} records processed")
        safe_log(f"Total tokens used: {total_tokens:,}, Estimated cost: ${total_cost:.6f}")

        return merged_df

    def _extract_sequential(
        self, df: pd.DataFrame, progress_interval: int, extract_func
    ) -> pd.DataFrame:
        """Sequential extraction with progress logging."""
        results = []

        for i, (_, row) in enumerate(df.iterrows()):
            if i % progress_interval == 0 and i > 0:
                safe_log(f"Processing item {i}/{len(df)} ({i/len(df)*100:.1f}%)")

            results.append(extract_func(row))

        return pd.DataFrame(results)

    def _extract_with_threading(
        self, df: pd.DataFrame, max_workers: int, progress_interval: int, extract_func
    ) -> pd.DataFrame:
        """Threaded extraction with progress tracking."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = [None] * len(df)
        completed_count = 0

        def extract_with_index(args):
            index, row = args
            return index, extract_func(row)

        # Create indexed data for maintaining order
        indexed_data = list(enumerate(df.iterrows()))
        indexed_rows = [(i, row) for i, (_, row) in indexed_data]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(extract_with_index, row_data): i
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
                    safe_log(f"Completed {completed_count}/{len(df)} extractions ({progress:.1f}%)")

        return pd.DataFrame(results)

    def _get_error_result(self, error_msg: str) -> Dict[str, Any]:
        """
        Return error result for entity extraction.

        Args:
            error_msg: Error message

        Returns:
            Dictionary with error result structure
        """
        return {
            "event_types": [],
            "locations": [],
            "people": [],
            "event_time": [],
            "reasoning": [f"Erro na extração: {error_msg}"],
            "extraction_type": "entity_extraction",
        }

    def get_extraction_stats(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics for entity extraction results.

        Args:
            results_df: DataFrame with extraction results

        Returns:
            Dictionary with extraction statistics
        """
        if results_df.empty:
            return {
                "total_records": 0,
                "avg_entities_per_record": 0.0,
                "most_common_event_types": {},
                "most_common_locations": {},
            }

        # Calculate entity statistics
        total_records = len(results_df)

        # Count entities per record
        entities_per_record = []
        for _, row in results_df.iterrows():
            count = (
                len(row.get("event_types", []))
                + len(row.get("locations", []))
                + len(row.get("people", []))
            )
            entities_per_record.append(count)

        avg_entities = (
            sum(entities_per_record) / len(entities_per_record) if entities_per_record else 0
        )

        return {
            "total_records": total_records,
            "avg_entities_per_record": avg_entities,
            "records_with_entities": len([x for x in entities_per_record if x > 0]),
            "entity_extraction_success_rate": (
                len([x for x in entities_per_record if x > 0]) / total_records * 100
                if total_records > 0
                else 0
            ),
        }

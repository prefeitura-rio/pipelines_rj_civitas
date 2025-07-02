# -*- coding: utf-8 -*-
"""
Public Safety Classifier Module

Binary classifier for determining if incidents are related to public safety.
Optimized for performance with efficient prompt design and minimal token usage.
"""

from typing import Any, Dict

import dspy
from prefeitura_rio.pipelines_utils.logging import log

from .base import BaseClassifier


class PublicSafetySignature(dspy.Signature):
    """Optimized signature for public safety binary classification."""

    descricao: str = dspy.InputField(desc="Descrição do evento/ocorrência a ser analisada")
    is_related: bool = dspy.OutputField(
        desc="True se relacionado à segurança pública (crimes, violência, emergências policiais), False caso contrário"
    )
    justification: str = dspy.OutputField(
        desc="Justificativa breve (máximo 1 frase) explicando a decisão"
    )


class PublicSafetyModule(dspy.Module):
    """Optimized DSPy module for public safety classification."""

    def __init__(self):
        super().__init__()
        # Use Predict instead of ChainOfThought for better performance
        self.classifier = dspy.Predict(PublicSafetySignature)

    def forward(self, descricao: str):
        """
        Classify if description is related to public safety.

        Args:
            descricao: Text description to classify

        Returns:
            Classification result with is_related and justification
        """
        return self.classifier(descricao=descricao)


class PublicSafetyClassifier(BaseClassifier):
    """
    Optimized binary classifier for public safety incidents.

    This classifier determines if an incident description is related to public safety
    concerns (crimes, violence, police emergencies) with high accuracy and performance.

    Features:
    - Binary classification (related/not related)
    - Optimized prompting for clarity
    - Comprehensive justification
    - Efficient error handling
    """

    def __init__(
        self,
        api_key: str | None = None,
        model_name: str = "gemini/gemini-2.5-flash",
        temperature: float = 0.3,
        max_tokens: int = 512,
        use_existing_dspy_config: bool = True,
    ):
        """
        Initialize the public safety classifier.

        Args:
            api_key: API key for the LLM
            model_name: Name of the LLM model
            temperature: Temperature for generation (lower for more consistent classification)
            max_tokens: Maximum tokens for response
            use_existing_dspy_config: Whether to use existing DSPy configuration
        """
        super().__init__(api_key, model_name, temperature, max_tokens, use_existing_dspy_config)

    def _setup_classifier(self):
        """Setup the optimized public safety classifier."""
        self.classificador = PublicSafetyModule()
        log(f"Public Safety Classifier initialized with {self.model_name}")
        log(f"Performance settings: temp={self.temperature}, max_tokens={self.max_tokens}")

    def classify_single(self, descricao: str) -> Dict[str, Any]:
        """
        Classify if description is related to public safety.

        Args:
            descricao: Text description to classify

        Returns:
            Dictionary with classification results:
            - is_related: Boolean indicating if related to public safety
            - justification: Brief explanation of the decision
            - classification_type: Always "public_safety"
        """
        if not descricao or not descricao.strip():
            return self._get_error_result("Empty description provided")

        try:
            response = self.classificador(descricao=descricao.strip())

            return {
                "is_related": getattr(response, "is_related", False),
                "justification": getattr(response, "justification", "").strip(),
                "classification_type": "public_safety",
            }

        except Exception as e:
            # Use try-catch for logging to avoid Prefect context issues
            try:
                log(f"Error classifying description: {str(e)}", level="error")
            except Exception as e:
                print(f"Error classifying description: {str(e)}")
            return self._get_error_result(str(e))

    def _get_error_result(self, error_msg: str) -> Dict[str, Any]:
        """
        Return error result for public safety classification.

        Args:
            error_msg: Error message

        Returns:
            Dictionary with error result structure
        """
        return {
            "is_related": False,
            "justification": f"Erro na classificação: {error_msg}",
            "classification_type": "public_safety",
        }

    def get_classification_stats(self, results_df) -> Dict[str, Any]:
        """
        Get statistics for public safety classification results.

        Args:
            results_df: DataFrame with classification results

        Returns:
            Dictionary with classification statistics
        """
        if results_df.empty:
            return {"total": 0, "related": 0, "not_related": 0, "related_percentage": 0.0}

        total = len(results_df)
        related = results_df["is_related"].sum()
        not_related = total - related

        return {
            "total": total,
            "related": related,
            "not_related": not_related,
            "related_percentage": (related / total * 100) if total > 0 else 0.0,
        }

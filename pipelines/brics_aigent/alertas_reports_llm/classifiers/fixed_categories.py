# -*- coding: utf-8 -*-
"""
Fixed Categories Classifier Module

Multi-category classifier for incident classification into predefined categories.
Includes training with few-shot examples for improved accuracy.
"""

from typing import Any, Dict, List, Optional

import dspy
from dspy.teleprompt import LabeledFewShot
from prefeitura_rio.pipelines_utils.logging import log

from .base import BaseClassifier


class FixedCategoriesSignature(dspy.Signature):
    """Signature for fixed category classification."""

    descricao: str = dspy.InputField(desc="Descrição livre da ocorrência policial")
    categorias: List[str] = dspy.OutputField(
        desc="Lista de categorias fixas que descrevem a ocorrência. Escolher apenas entre as permitidas."
    )


class FixedCategoriesModule(dspy.Module):
    """DSPy module for fixed category classification."""

    def __init__(self):
        super().__init__()
        self.classifier = dspy.Predict(FixedCategoriesSignature)

    def forward(self, descricao: str):
        """
        Classify description into fixed categories.

        Args:
            descricao: Text description to classify

        Returns:
            Classification result with categories list
        """
        return self.classifier(descricao=descricao)


class FixedCategoriesClassifier(BaseClassifier):
    """
    Multi-category classifier for incident classification into predefined categories.

    This classifier uses few-shot learning with training examples to improve
    accuracy in categorizing incidents into fixed categories.
    """

    DEFAULT_CATEGORIES = [
        "Violência Urbana",
        "Crimes Patrimoniais",
        "Tráfico e Drogas",
        "Facções, Milícias e Armas",
        "Ameaças e Risco Coletivo",
        "Ações Policiais e Ocorrências",
        "Infrações e Conduta Irregular",
        "Segurança Pública e Falta de Policiamento",
        "Terrorismo",
        "Sem classificação",
    ]

    def __init__(
        self,
        api_key: str = None,
        model_name: str = "gemini/gemini-2.5-flash",
        temperature: float = 0.5,
        max_tokens: int = 1024,
        fixed_categories: Optional[List[str]] = None,
        use_existing_dspy_config: bool = True,
    ):
        """
        Initialize the fixed categories classifier.

        Args:
            api_key: API key for the LLM service
            model_name: Name of the LLM model to use
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens to generate
            fixed_categories: List of categories to use (uses defaults if None)
            use_existing_dspy_config: Whether to use existing DSPy configuration
        """
        self.fixed_categories = fixed_categories or self.DEFAULT_CATEGORIES.copy()
        super().__init__(api_key, model_name, temperature, max_tokens, use_existing_dspy_config)

    def _setup_classifier(self):
        """Setup the fixed categories classifier with training."""
        log(f"Setting up Fixed Categories Classifier with {len(self.fixed_categories)} categories")
        log(f"Categories: {', '.join(self.fixed_categories)}")

        self._train_classifier()
        log(f"Fixed Categories Classifier initialized with {self.model_name}")

    def _create_training_examples(self):
        """
        Create training examples for the classifier.

        Returns:
            List of DSPy examples for few-shot learning
        """
        training_data = [
            (
                "Mulher é agredida com socos pelo companheiro dentro de casa, vizinhos chamam a polícia.",
                ["Violência Urbana"],
            ),
            (
                "Homem armado rende funcionários de mercado e rouba dinheiro do caixa.",
                ["Crimes Patrimoniais"],
            ),
            (
                "Adolescente é flagrado com entorpecentes em mochila perto de escola.",
                ["Tráfico e Drogas"],
            ),
            (
                "Moradores relatam presença de milicianos armados com fuzis patrulhando a região.",
                ["Facções, Milícias e Armas"],
            ),
            (
                "Criança é atingida por bala perdida durante confronto entre facção e polícia.",
                ["Ameaças e Risco Coletivo"],
            ),
            (
                "PM realiza operação no morro e prende três suspeitos após troca de tiros.",
                ["Ações Policiais e Ocorrências"],
            ),
            (
                "Motorista alcoolizado é flagrado dirigindo em alta velocidade durante a madrugada.",
                ["Infrações e Conduta Irregular"],
            ),
            (
                "Moradores denunciam falta de policiamento noturno no bairro e aumento da criminalidade.",
                ["Segurança Pública e Falta de Policiamento"],
            ),
            (
                "Suspeito tenta detonar artefato explosivo em estação de metrô movimentada.",
                ["Terrorismo"],
            ),
            (
                "Cachorro solto na rua está latindo muito alto e incomodando os vizinhos.",
                ["Sem classificação"],
            ),
        ]

        # Filter examples to only include categories that are in our fixed_categories
        filtered_examples = []
        for desc, cats in training_data:
            filtered_cats = [cat for cat in cats if cat in self.fixed_categories]
            if filtered_cats:  # Only include if at least one category matches
                filtered_examples.append((desc, filtered_cats))

        return [
            dspy.Example(descricao=desc, categorias=cats).with_inputs("descricao")
            for desc, cats in filtered_examples
        ]

    def _train_classifier(self):
        """Train the classifier with few-shot examples."""
        training_examples = self._create_training_examples()

        if not training_examples:
            log("No valid training examples found, using base classifier", level="warning")
            self.classificador_tunado = FixedCategoriesModule()
            return

        log(f"Training classifier with {len(training_examples)} examples")

        # Use LabeledFewShot optimizer for training
        optimizer = LabeledFewShot(
            k=min(len(training_examples), 5)
        )  # Limit to 5 examples for performance
        base_module = FixedCategoriesModule()

        try:
            self.classificador_tunado = optimizer.compile(
                student=base_module, trainset=training_examples
            )
            log("Classifier training completed successfully")
        except Exception as e:
            log(f"Error during training, using base classifier: {e}", level="warning")
            self.classificador_tunado = base_module

    def classify_single(self, descricao: str) -> Dict[str, Any]:
        """
        Classify description into fixed categories.

        Args:
            descricao: Text description to classify

        Returns:
            Dictionary with classification results:
            - categorias: List of assigned categories
            - classification_type: Always "fixed_categories"
        """
        if not descricao or not descricao.strip():
            return self._get_error_result("Empty description provided")

        try:
            response = self.classificador_tunado(descricao=descricao.strip())
            categorias = getattr(response, "categorias", [])

            # Ensure categorias is a list and filter valid ones
            if not isinstance(categorias, list):
                categorias = [categorias] if categorias else []

            # Filter to only include valid categories
            valid_categorias = [cat for cat in categorias if cat in self.fixed_categories]

            # If no valid categories, assign default
            if not valid_categorias:
                valid_categorias = ["Sem classificação"]

            return {"categorias": valid_categorias, "classification_type": "fixed_categories"}

        except Exception as e:
            # Use try-catch for logging to avoid Prefect context issues
            try:
                log(f"Error classifying description: {str(e)}", level="error")
            except Exception as e:
                print(f"Error classifying description: {str(e)}")
            return self._get_error_result(str(e))

    def _get_error_result(self, error_msg: str) -> Dict[str, Any]:
        """
        Return error result for fixed categories classification.

        Args:
            error_msg: Error message

        Returns:
            Dictionary with error result structure
        """
        return {"categorias": ["Sem classificação"], "classification_type": "fixed_categories"}

    def get_categories_summary(self, results_df) -> Dict[str, Any]:
        """
        Get summary of category distribution in results.

        Args:
            results_df: DataFrame with classification results

        Returns:
            Dictionary with category statistics
        """
        if results_df.empty:
            return {"total_records": 0, "category_counts": {}, "unique_categories": 0}

        # Explode the categorias column to count individual categories
        exploded_df = results_df.explode("categorias")
        category_counts = exploded_df["categorias"].value_counts().to_dict()

        return {
            "total_records": len(results_df),
            "category_counts": category_counts,
            "unique_categories": len(category_counts),
            "most_common_category": (
                max(category_counts.items(), key=lambda x: x[1]) if category_counts else None
            ),
        }

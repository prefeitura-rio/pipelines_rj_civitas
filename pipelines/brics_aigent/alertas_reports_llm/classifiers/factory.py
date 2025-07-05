# -*- coding: utf-8 -*-
"""
Classifier Factory Module

Factory class for creating and managing different types of BRICS LLM classifiers.
Provides a centralized way to instantiate classifiers with proper configuration.
"""

from typing import Any, Dict, Type

from prefeitura_rio.pipelines_utils.logging import log

from .base import BaseClassifier
from .context_relevance import ContextRelevanceClassifier
from .entity_extractor import EntityExtractionClassifier
from .fixed_categories import FixedCategoriesClassifier
from .public_safety import PublicSafetyClassifier


class ClassifierFactory:
    """
    Factory class for creating BRICS LLM classifiers.

    Provides methods to create different types of classifiers with consistent
    configuration and error handling.
    """

    # Registry of available classifier types
    CLASSIFIER_TYPES: Dict[str, Type[BaseClassifier]] = {
        "public_safety": PublicSafetyClassifier,
        "fixed_categories": FixedCategoriesClassifier,
        "entity_extraction": EntityExtractionClassifier,
        "context_relevance": ContextRelevanceClassifier,
    }

    @classmethod
    def get_available_types(cls) -> list[str]:
        """
        Get list of available classifier types.

        Returns:
            List of available classifier type names
        """
        return list(cls.CLASSIFIER_TYPES.keys())

    @classmethod
    def create_classifier(
        cls, classifier_type: str, use_existing_dspy_config: bool = True, **kwargs
    ) -> BaseClassifier:
        """
        Create a classifier instance based on the specified type.

        Args:
            classifier_type: Type of classifier to create
            use_existing_dspy_config: Whether to use existing DSPy configuration
            **kwargs: Additional arguments for classifier initialization

        Returns:
            Classifier instance

        Raises:
            ValueError: If classifier_type is not supported
        """
        if classifier_type not in cls.CLASSIFIER_TYPES:
            available_types = ", ".join(cls.CLASSIFIER_TYPES.keys())
            raise ValueError(
                f"Unknown classifier type: '{classifier_type}'. "
                f"Available types: {available_types}"
            )

        # Add the flag to kwargs
        kwargs["use_existing_dspy_config"] = use_existing_dspy_config

        classifier_class = cls.CLASSIFIER_TYPES[classifier_type]

        try:
            classifier = classifier_class(**kwargs)
            log(f"Successfully created {classifier_type} classifier")
            return classifier
        except Exception as e:
            # Use try-catch for logging to avoid Prefect context issues
            try:
                log(f"Error creating {classifier_type} classifier: {e}", level="error")
            except Exception as e:
                print(f"Error creating {classifier_type} classifier: {e}")
            raise

    @classmethod
    def create_public_safety_classifier(cls, **kwargs) -> PublicSafetyClassifier:
        """
        Create a public safety binary classifier.

        Args:
            **kwargs: Arguments to pass to PublicSafetyClassifier constructor

        Returns:
            Configured PublicSafetyClassifier instance
        """
        return cls.create_classifier("public_safety", **kwargs)

    @classmethod
    def create_fixed_categories_classifier(cls, **kwargs) -> FixedCategoriesClassifier:
        """
        Create a fixed categories multi-label classifier.

        Args:
            **kwargs: Arguments to pass to FixedCategoriesClassifier constructor

        Returns:
            Configured FixedCategoriesClassifier instance
        """
        return cls.create_classifier("fixed_categories", **kwargs)

    @classmethod
    def create_entity_extraction_classifier(cls, **kwargs) -> EntityExtractionClassifier:
        """
        Create an entity and time extraction classifier.

        Args:
            **kwargs: Arguments to pass to EntityExtractionClassifier constructor

        Returns:
            Configured EntityExtractionClassifier instance
        """
        return cls.create_classifier("entity_extraction", **kwargs)

    @classmethod
    def create_context_relevance_classifier(cls, **kwargs) -> ContextRelevanceClassifier:
        """
        Create a context relevance analysis classifier.

        Args:
            **kwargs: Arguments to pass to ContextRelevanceClassifier constructor

        Returns:
            Configured ContextRelevanceClassifier instance
        """
        return cls.create_classifier("context_relevance", **kwargs)

    @classmethod
    def register_classifier(cls, name: str, classifier_class: Type[BaseClassifier]) -> None:
        """
        Register a new classifier type.

        Args:
            name: Name for the classifier type
            classifier_class: Classifier class that inherits from BaseClassifier

        Raises:
            ValueError: If classifier_class doesn't inherit from BaseClassifier
        """
        if not issubclass(classifier_class, BaseClassifier):
            raise ValueError(
                f"Classifier class must inherit from BaseClassifier, "
                f"got {classifier_class.__name__}"
            )

        cls.CLASSIFIER_TYPES[name] = classifier_class
        log(f"Registered new classifier type: {name}")

    @classmethod
    def get_classifier_info(cls, classifier_type: str) -> Dict[str, Any]:
        """
        Get information about a specific classifier type.

        Args:
            classifier_type: Type of classifier to get info for

        Returns:
            Dictionary with classifier information

        Raises:
            ValueError: If classifier_type is not supported
        """
        if classifier_type not in cls.CLASSIFIER_TYPES:
            available_types = ", ".join(cls.CLASSIFIER_TYPES.keys())
            raise ValueError(
                f"Unknown classifier type: '{classifier_type}'. "
                f"Available types: {available_types}"
            )

        classifier_class = cls.CLASSIFIER_TYPES[classifier_type]

        return {
            "type": classifier_type,
            "class_name": classifier_class.__name__,
            "module": classifier_class.__module__,
            "docstring": classifier_class.__doc__,
        }

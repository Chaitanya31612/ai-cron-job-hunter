import os
import logging
from typing import Optional
from .base import AIProvider, AIAnalyzerInterface, AIResponse
from .groq_analyzer import GroqAnalyzer
from .gemini_analyzer import GeminiAnalyzer
from .openrouter_analyzer import OpenRouterAnalyzer

logger = logging.getLogger(__name__)


class AIAnalyzerFactory:
    """
    Factory for creating AI analyzer instances with automatic fallback.

    Provider priority (highest quality / most generous free tier first):
        1. Gemini 3.1 Flash-Lite  — 500 RPD free
        2. OpenRouter Llama 3.3 70B  — 50 RPD free
        3. OpenRouter GPT-OSS 120B   — 50 RPD free (next free model slot)
        4. Groq Llama 3.3 70B        — last resort (100K tokens/day cap)

    Usage:
        # Explicit provider:
        analyzer = AIAnalyzerFactory.create(AIProvider.GEMINI)

        # Auto-select best available from env keys:
        analyzer = AIAnalyzerFactory.create_default()

        # Full cascading fallback (recommended for production):
        result = AIAnalyzerFactory.analyze_with_fallback(content, prompt)
    """

    _registry: dict[AIProvider, type[AIAnalyzerInterface]] = {
        AIProvider.GEMINI: GeminiAnalyzer,
        AIProvider.OPENROUTER: OpenRouterAnalyzer,
        AIProvider.GROQ: GroqAnalyzer,
    }

    # Errors that indicate a rate-limit or quota exhaustion — trigger fallback
    _RATE_LIMIT_SIGNALS = (
        "429",
        "rate limit",
        "quota",
        "resource exhausted",
        "too many requests",
        "ratelimit",
    )

    @classmethod
    def register(cls, provider: AIProvider, analyzer_class: type[AIAnalyzerInterface]):
        """Register a new analyzer class for a provider."""
        cls._registry[provider] = analyzer_class

    @classmethod
    def create(
        cls,
        provider: AIProvider,
        model: Optional[str] = None,
        **kwargs,
    ) -> AIAnalyzerInterface:
        """
        Create an analyzer for the specified provider.

        Args:
            provider: The AI provider to use
            model: Optional model name override (uses provider default if omitted)
            **kwargs: Additional arguments passed to the analyzer constructor
        """
        if provider not in cls._registry:
            raise ValueError(
                f"Unknown provider: {provider}. "
                f"Available: {list(cls._registry.keys())}"
            )

        analyzer_class = cls._registry[provider]
        if model:
            return analyzer_class(model=model, **kwargs)
        return analyzer_class(**kwargs)

    @classmethod
    def create_default(cls) -> AIAnalyzerInterface:
        """
        Create the best available analyzer based on which API keys are set.
        Priority: Gemini > OpenRouter > Groq
        """
        if os.getenv("GEMINI_API_KEY"):
            return cls.create(AIProvider.GEMINI)

        if os.getenv("OPENROUTER_API_KEY"):
            return cls.create(AIProvider.OPENROUTER)

        if os.getenv("GROQ_API_KEY"):
            return cls.create(AIProvider.GROQ)

        raise RuntimeError(
            "No AI provider API keys found. "
            "Set at least one of: GEMINI_API_KEY, OPENROUTER_API_KEY, GROQ_API_KEY"
        )

    @classmethod
    def available_providers(cls) -> list[AIProvider]:
        """List providers that have valid API key configuration."""
        available = []
        if os.getenv("GEMINI_API_KEY"):
            available.append(AIProvider.GEMINI)
        if os.getenv("OPENROUTER_API_KEY"):
            available.append(AIProvider.OPENROUTER)
        if os.getenv("GROQ_API_KEY"):
            available.append(AIProvider.GROQ)
        return available

    @classmethod
    def _is_rate_limit_error(cls, error_str: str) -> bool:
        """Check if an error string looks like a rate-limit / quota error."""
        lowered = error_str.lower()
        return any(signal in lowered for signal in cls._RATE_LIMIT_SIGNALS)

    @classmethod
    def analyze_with_fallback(
        cls,
        content: str,
        prompt: str,
    ) -> AIResponse:
        """
        Run analysis with automatic cascading fallback.

        Tries each provider in priority order. Falls through to the next
        provider only on rate-limit / quota errors (429, ResourceExhausted, etc.)
        Hard errors (auth failures, bad requests) are returned immediately.

        Fallback chain:
            Gemini 3.1 Flash-Lite
                → OpenRouter Llama 3.3 70B (free)
                → OpenRouter GPT-OSS 120B  (free, next model slot)
                → Groq Llama 3.3 70B
                → All exhausted: return last error response

        Returns:
            AIResponse from the first provider that succeeds.
        """
        attempts: list[AIAnalyzerInterface] = []

        # Build ordered list of analyzers to try
        if os.getenv("GEMINI_API_KEY"):
            try:
                attempts.append(cls.create(AIProvider.GEMINI))
            except Exception as e:
                logger.warning(f"Could not initialise Gemini: {e}")

        if os.getenv("OPENROUTER_API_KEY"):
            try:
                # Add all free OpenRouter models in sequence
                for model in OpenRouterAnalyzer.FREE_MODELS:
                    attempts.append(OpenRouterAnalyzer(model=model))
            except Exception as e:
                logger.warning(f"Could not initialise OpenRouter: {e}")

        if os.getenv("GROQ_API_KEY"):
            try:
                # Use the smart 70B model as last resort
                attempts.append(GroqAnalyzer(model="smart"))
            except Exception as e:
                logger.warning(f"Could not initialise Groq: {e}")

        if not attempts:
            return AIResponse(
                content="",
                model="none",
                provider=AIProvider.GEMINI,
                success=False,
                error="No AI providers configured. Set at least one API key.",
            )

        last_response: Optional[AIResponse] = None

        for analyzer in attempts:
            provider_label = f"{analyzer.provider.value}/{analyzer.model_name}"
            logger.info(f"Trying provider: {provider_label}")

            response = analyzer.analyze(content, prompt)

            if response.success:
                logger.info(f"✅ Success with {provider_label}")
                return response

            # Fall back to the next provider/model on any error (rate limit, 404, auth error, etc.)
            logger.warning(
                f"⚠️  Error from {provider_label}: {response.error or 'Unknown error'}. "
                f"Falling back to next provider..."
            )
            last_response = response
            continue

        # All providers exhausted
        logger.error("❌ All providers exhausted or rate-limited.")
        return last_response or AIResponse(
            content="",
            model="none",
            provider=AIProvider.GEMINI,
            success=False,
            error="All providers exhausted.",
        )

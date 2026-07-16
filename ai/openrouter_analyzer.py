import os
from openai import OpenAI
from .base import AIAnalyzerInterface, AIResponse, AIProvider


class OpenRouterAnalyzer(AIAnalyzerInterface):
    """
    OpenRouter API implementation using the OpenAI-compatible SDK.

    OpenRouter provides free access to high-quality open-weight models
    (e.g., Llama 3.3 70B, GPT-OSS 120B) via a single API key.

    Free tier limits (standard account, no credits purchased):
        - 50 requests per day across all :free models
        - 20 requests per minute

    Model IDs use the ':free' suffix to target the free routing tier.
    """

    # Ordered list of free models — tried top-to-bottom on failure
    FREE_MODELS = [
        "meta-llama/llama-3.3-70b-instruct:free",   # Primary: strong 70B reasoning
        "google/gemma-4-31b-it:free",                # Fallback: strong Google model
        "qwen/qwen3-coder:free",                      # Coding/reasoning model
    ]

    def __init__(self, model: str = None):
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY not found in environment")

        self.client = OpenAI(
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            max_retries=0,
            default_headers={
                # Required by OpenRouter: identifies your app in their dashboard
                "HTTP-Referer": "https://github.com/ai-job-hunter",
                "X-Title": "AI Job Hunter",
            },
        )

        # Allow explicit model override; default to first FREE_MODEL
        self._model = model if model else self.FREE_MODELS[0]
        self._provider = AIProvider.OPENROUTER

    @property
    def provider(self) -> AIProvider:
        return self._provider

    @property
    def model_name(self) -> str:
        return self._model

    def analyze(self, content: str, prompt: str) -> AIResponse:
        try:
            response = self.client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": content},
                ],
                max_tokens=2048,
                temperature=0.1,
            )

            tokens_used = 0
            if response.usage:
                tokens_used = response.usage.total_tokens

            return AIResponse(
                content=response.choices[0].message.content,
                model=self._model,
                provider=self._provider,
                tokens_used=tokens_used,
            )

        except Exception as e:
            return AIResponse(
                content="",
                model=self._model,
                provider=self._provider,
                success=False,
                error=str(e),
            )

    def with_next_model(self) -> "OpenRouterAnalyzer | None":
        """
        Return a new OpenRouterAnalyzer configured with the next model
        in the FREE_MODELS list, or None if already on the last model.
        Used by the factory fallback chain to try the next free model
        when the current one returns a 429 or error.
        """
        try:
            current_idx = self.FREE_MODELS.index(self._model)
            next_idx = current_idx + 1
            if next_idx < len(self.FREE_MODELS):
                return OpenRouterAnalyzer(model=self.FREE_MODELS[next_idx])
        except ValueError:
            pass
        return None

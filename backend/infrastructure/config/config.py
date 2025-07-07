import os
from pydantic_settings import BaseSettings
from typing import Dict, List
from pathlib import Path

def find_env_file():
    """Find the .env file by looking up the directory tree"""
    current_dir = Path(__file__).resolve()
    
    # Look for .env file starting from backend directory and moving up
    backend_dir = current_dir.parent.parent  # backend directory
    for parent in [backend_dir, backend_dir.parent, current_dir.parent, current_dir]:
        env_file = parent / ".env"
        if env_file.exists():
            return str(env_file)
    
    # Default to .env in the backend directory
    return str(backend_dir / ".env")

class Settings(BaseSettings):
    # Database and Redis
    DATABASE_URL: str = "sqlite:///./sql_app.db"
    REDIS_URL: str = "redis://localhost:6379"
    
    # Primary API Keys
    GEMINI_API_KEY: str = ""
    OPENAI_API_KEY: str = ""
    OPENROUTER_API_KEY: str = ""
    
    # Multiple OpenRouter API Keys (for rate limiting and fallback)
    OPENROUTER_API_KEY_1: str = ""
    OPENROUTER_API_KEY_2: str = ""
    OPENROUTER_API_KEY_3: str = ""
    
    # Multiple Gemini API Keys (for rate limiting and fallback)
    GEMINI_API_KEY_1: str = ""
    GEMINI_API_KEY_2: str = ""
    GEMINI_API_KEY_3: str = ""
    
    # Model Configuration
    DEFAULT_GEMINI_MODEL: str = "gemini-2.0-flash"
    DEFAULT_OPENROUTER_MODEL: str = "deepseek/deepseek-r1-0528-qwen3-8b:free"
    DEFAULT_OPENAI_MODEL: str = "gpt-4o"
    
    # Rate Limiting
    REQUESTS_PER_MINUTE: int = 60
    MAX_TOKENS_PER_REQUEST: int = 4000
    
    # Fallback Configuration
    ENABLE_FALLBACK_MODELS: bool = True
    RETRY_ATTEMPTS: int = 3
    
    def get_openrouter_keys(self) -> List[str]:
        """Returns all configured OpenRouter API keys"""
        keys = []
        if self.OPENROUTER_API_KEY:
            keys.append(self.OPENROUTER_API_KEY)
        if self.OPENROUTER_API_KEY_1:
            keys.append(self.OPENROUTER_API_KEY_1)
        if self.OPENROUTER_API_KEY_2:
            keys.append(self.OPENROUTER_API_KEY_2)
        if self.OPENROUTER_API_KEY_3:
            keys.append(self.OPENROUTER_API_KEY_3)
        return [key for key in keys if key]
    
    def get_gemini_keys(self) -> List[str]:
        """Returns all configured Gemini API keys"""
        keys = []
        if self.GEMINI_API_KEY:
            keys.append(self.GEMINI_API_KEY)
        if self.GEMINI_API_KEY_1:
            keys.append(self.GEMINI_API_KEY_1)
        if self.GEMINI_API_KEY_2:
            keys.append(self.GEMINI_API_KEY_2)
        if self.GEMINI_API_KEY_3:
            keys.append(self.GEMINI_API_KEY_3)
        return [key for key in keys if key]

    class Config:
        env_file = find_env_file()
        extra = "ignore"

settings = Settings()

# Debug: Print loaded configuration (remove in production)
print(f"🔧 Configuration loaded from: {settings.Config.env_file}")
print(f"📊 API Keys detected:")
print(f"  - OpenRouter: {len(settings.get_openrouter_keys())} keys")
print(f"  - Gemini: {len(settings.get_gemini_keys())} keys")
print(f"  - OpenAI: {1 if settings.OPENAI_API_KEY else 0} keys")
print(f"🎯 Default models:")
print(f"  - OpenRouter: {settings.DEFAULT_OPENROUTER_MODEL}")
print(f"  - Gemini: {settings.DEFAULT_GEMINI_MODEL}")
print(f"  - OpenAI: {settings.DEFAULT_OPENAI_MODEL}")



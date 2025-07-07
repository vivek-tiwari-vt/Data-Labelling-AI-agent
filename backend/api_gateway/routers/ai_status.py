from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.utils.ai_client import AIClient, ModelProvider
from infrastructure.config.config import settings

router = APIRouter()
ai_client = AIClient()

@router.get("/status")
async def get_ai_status():
    """Get the current status of all AI providers and API keys"""
    try:
        status = ai_client.get_status()
        return {
            "status": "success",
            "data": status,
            "configuration": {
                "default_models": {
                    "gemini": settings.DEFAULT_GEMINI_MODEL,
                    "openrouter": settings.DEFAULT_OPENROUTER_MODEL,
                    "openai": settings.DEFAULT_OPENAI_MODEL
                },
                "rate_limiting": {
                    "requests_per_minute": settings.REQUESTS_PER_MINUTE,
                    "max_tokens_per_request": settings.MAX_TOKENS_PER_REQUEST
                },
                "fallback_enabled": settings.ENABLE_FALLBACK_MODELS,
                "retry_attempts": settings.RETRY_ATTEMPTS
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get AI status: {str(e)}")

@router.get("/models/{provider}")
async def get_available_models(provider: str):
    """Get available models for a specific provider"""
    try:
        provider_enum = ModelProvider(provider.lower())
        models = ai_client.get_available_models(provider_enum)
        return {
            "status": "success",
            "provider": provider,
            "models": models
        }
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid provider: {provider}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get models: {str(e)}")

@router.post("/test/{provider}")
async def test_provider(provider: str, test_prompt: str = "Hello, this is a test prompt"):
    """Test a specific AI provider with a simple prompt"""
    try:
        provider_enum = ModelProvider(provider.lower())
        
        result = await ai_client.generate_completion(
            prompt=test_prompt,
            provider=provider_enum
        )
        
        return {
            "status": "success",
            "provider": provider,
            "test_prompt": test_prompt,
            "result": result
        }
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid provider: {provider}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Test failed: {str(e)}")

@router.post("/test-all")
async def test_all_providers(test_prompt: str = "Hello, this is a test prompt"):
    """Test all available AI providers"""
    results = {}
    
    for provider in ModelProvider:
        try:
            result = await ai_client.generate_completion(
                prompt=test_prompt,
                provider=provider
            )
            results[provider.value] = {
                "status": "success",
                "result": result
            }
        except Exception as e:
            results[provider.value] = {
                "status": "failed",
                "error": str(e)
            }
    
    return {
        "status": "completed",
        "test_prompt": test_prompt,
        "results": results
    }

@router.get("/keys/validation")
async def validate_api_keys():
    """Validate which API keys are configured (without exposing the actual keys)"""
    
    openrouter_keys = settings.get_openrouter_keys()
    gemini_keys = settings.get_gemini_keys()
    openai_keys = [settings.OPENAI_API_KEY] if settings.OPENAI_API_KEY else []
    
    def mask_key(key: str) -> str:
        """Mask API key for security"""
        if len(key) <= 8:
            return "*" * len(key)
        return key[:4] + "*" * (len(key) - 8) + key[-4:]
    
    return {
        "status": "success",
        "api_keys": {
            "openrouter": {
                "count": len(openrouter_keys),
                "keys": [mask_key(key) for key in openrouter_keys if key]
            },
            "gemini": {
                "count": len(gemini_keys),
                "keys": [mask_key(key) for key in gemini_keys if key]
            },
            "openai": {
                "count": len(openai_keys),
                "keys": [mask_key(key) for key in openai_keys if key]
            }
        }
    } 
import os
import asyncio
import random
import json
from enum import Enum
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from .config import settings
# import google.generativeai as genai # Uncomment when integrating actual LLM
# from openai import OpenAI # Uncomment when integrating actual LLM

class ModelProvider(Enum):
    GEMINI = "gemini"
    OPENAI = "openai"
    OPENROUTER = "openrouter"

class APIKeyManager:
    """Manages multiple API keys with rotation and fallback"""
    
    def __init__(self):
        self.openrouter_keys = settings.get_openrouter_keys()
        self.gemini_keys = settings.get_gemini_keys()
        self.openai_keys = [settings.OPENAI_API_KEY] if settings.OPENAI_API_KEY else []
        
        # Track usage for rate limiting
        self.key_usage = {}
        self.last_reset = datetime.now()
    
    def _reset_usage_if_needed(self):
        """Reset usage counters if a minute has passed"""
        now = datetime.now()
        if now - self.last_reset >= timedelta(minutes=1):
            self.key_usage.clear()
            self.last_reset = now
    
    def get_available_key(self, provider: ModelProvider) -> Optional[str]:
        """Get an available API key for the specified provider"""
        self._reset_usage_if_needed()
        
        if provider == ModelProvider.OPENROUTER:
            keys = self.openrouter_keys
        elif provider == ModelProvider.GEMINI:
            keys = self.gemini_keys
        elif provider == ModelProvider.OPENAI:
            keys = self.openai_keys
        else:
            return None
        
        if not keys:
            return None
        
        # Find keys that haven't exceeded rate limits
        available_keys = []
        for key in keys:
            usage_count = self.key_usage.get(key, 0)
            if usage_count < settings.REQUESTS_PER_MINUTE:
                available_keys.append(key)
        
        if not available_keys:
            # If all keys are rate limited, return the least used one
            available_keys = keys
        
        # Randomly select from available keys for load balancing
        selected_key = random.choice(available_keys)
        
        # Track usage
        self.key_usage[selected_key] = self.key_usage.get(selected_key, 0) + 1
        
        return selected_key

class AIClient:
    def __init__(self):
        self.key_manager = APIKeyManager()
        self.gemini_clients = {}
        self.openai_clients = {}
        self.openrouter_clients = {}
        self.config = settings  # Add config reference for compatibility
        self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize AI clients for all available providers"""
        # Placeholder for actual client initialization
        # This would be uncommented when integrating real LLM APIs
        
        # # Initialize Gemini clients
        # for key in self.key_manager.gemini_keys:
        #     try:
        #         genai.configure(api_key=key)
        #         self.gemini_clients[key] = genai.GenerativeModel(settings.DEFAULT_GEMINI_MODEL)
        #     except Exception as e:
        #         print(f"Failed to initialize Gemini client with key: {e}")
        
        # # Initialize OpenAI clients
        # for key in self.key_manager.openai_keys:
        #     try:
        #         self.openai_clients[key] = OpenAI(api_key=key)
        #     except Exception as e:
        #         print(f"Failed to initialize OpenAI client with key: {e}")
        
        # # Initialize OpenRouter clients
        # for key in self.key_manager.openrouter_keys:
        #     try:
        #         self.openrouter_clients[key] = OpenAI(
        #             base_url="https://openrouter.ai/api/v1",
        #             api_key=key
        #         )
        #     except Exception as e:
        #         print(f"Failed to initialize OpenRouter client with key: {e}")
        
        print(f"AI Client initialized with:")
        print(f"  - {len(self.key_manager.openrouter_keys)} OpenRouter keys")
        print(f"  - {len(self.key_manager.gemini_keys)} Gemini keys")
        print(f"  - {len(self.key_manager.openai_keys)} OpenAI keys")
    
    async def generate_completion(
        self,
        prompt: str,
        provider: ModelProvider = ModelProvider.OPENROUTER,
        model: Optional[str] = None,
        images: Optional[List[bytes]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Generate completion with smart key rotation and retry logic"""
        
        # Set default models
        if not model:
            if provider == ModelProvider.GEMINI:
                model = settings.DEFAULT_GEMINI_MODEL
            elif provider == ModelProvider.OPENROUTER:
                model = settings.DEFAULT_OPENROUTER_MODEL
            elif provider == ModelProvider.OPENAI:
                model = settings.DEFAULT_OPENAI_MODEL
        
        # Get ALL available keys for this provider
        all_keys = self._get_all_keys_for_provider(provider)
        if not all_keys:
            raise Exception(f"❌ NO API KEYS CONFIGURED for {provider.value}! Please configure API keys in .env file. Run: python setup_api_keys.py")
        
        print(f"🔑 Found {len(all_keys)} API key(s) for {provider.value}")
        
        # Try each key until one works
        last_error = None
        for key_index, api_key in enumerate(all_keys):
            try:
                print(f"🔄 Trying API key {key_index + 1}/{len(all_keys)} for {provider.value}")
                
                # Make real API call
                result = await self._make_api_call(provider, api_key, model, prompt, images, **kwargs)
                print(f"✅ Success with key {key_index + 1}/{len(all_keys)}")
                return result
                
            except Exception as e:
                last_error = e
                print(f"❌ Key {key_index + 1}/{len(all_keys)} failed: {str(e)[:100]}...")
                
                # If this isn't the last key, continue to next key
                if key_index < len(all_keys) - 1:
                    print(f"🔄 Trying next API key...")
                    await asyncio.sleep(0.5)  # Brief pause between key attempts
                    continue
        
        # All keys exhausted
        raise Exception(f"❌ ALL {len(all_keys)} API KEYS FAILED for {provider.value}! Last error: {last_error}")
    
    def _get_all_keys_for_provider(self, provider: ModelProvider) -> List[str]:
        """Get all available API keys for a provider"""
        if provider == ModelProvider.OPENROUTER:
            return [key for key in self.key_manager.openrouter_keys if key.strip()]
        elif provider == ModelProvider.GEMINI:
            return [key for key in self.key_manager.gemini_keys if key.strip()]
        elif provider == ModelProvider.OPENAI:
            return [key for key in self.key_manager.openai_keys if key.strip()]
        else:
            return []
    
    async def _make_api_call(
        self,
        provider: ModelProvider,
        api_key: str,
        model: str,
        prompt: str,
        images: Optional[List[bytes]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Make REAL API calls to the actual AI services"""
        
        print(f"🔥 REAL API call to {provider.value} with model {model}")
        print(f"🔑 Using API key: {api_key[:15]}...")
        
        try:
            if provider == ModelProvider.OPENROUTER:
                return await self._call_openrouter_api(api_key, model, prompt, **kwargs)
            elif provider == ModelProvider.GEMINI:
                return await self._call_gemini_api(api_key, model, prompt, **kwargs)
            elif provider == ModelProvider.OPENAI:
                return await self._call_openai_api(api_key, model, prompt, **kwargs)
            else:
                raise ValueError(f"Unsupported provider: {provider}")
                
        except Exception as e:
            print(f"❌ API call failed: {str(e)}")
            # Return error instead of fallback
            raise Exception(f"API call to {provider.value} failed: {str(e)}")
    
    async def _call_openrouter_api(self, api_key: str, model: str, prompt: str, **kwargs) -> Dict[str, Any]:
        """Make real OpenRouter API call"""
        import aiohttp
        
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/your-repo",
            "X-Title": "Data Labeling Agent"
        }
        
        # Convert prompt to messages format
        messages = [{"role": "user", "content": prompt}]
        
        data = {
            "model": model,
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", 1000),
            "temperature": kwargs.get("temperature", 0.1),
        }
        
        print(f"📡 Calling OpenRouter API: {model}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result["choices"][0]["message"]["content"]
                    
                    print(f"✅ OpenRouter response received: {len(content)} chars")
                    
                    return {
                        "generated_text": content,
                        "provider": "openrouter",
                        "model": model,
                        "confidence": 0.9
                    }
                else:
                    error_text = await response.text()
                    raise Exception(f"OpenRouter API error {response.status}: {error_text}")
    
    async def _call_gemini_api(self, api_key: str, model: str, prompt: str, **kwargs) -> Dict[str, Any]:
        """Make real Gemini API call"""
        import aiohttp
        
        # Map model names
        if model.startswith("gemini-1.5-pro"):
            api_model = "gemini-1.5-pro-latest"
        elif model.startswith("gemini-1.5-flash"):
            api_model = "gemini-1.5-flash-latest"
        else:
            api_model = "gemini-1.5-pro-latest"
        
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{api_model}:generateContent?key={api_key}"
        headers = {
            "Content-Type": "application/json"
        }
        
        data = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "generationConfig": {
                "temperature": kwargs.get("temperature", 0.1),
                "maxOutputTokens": kwargs.get("max_tokens", 1000),
                "topP": 0.8,
                "topK": 10
            }
        }
        
        print(f"📡 Calling Gemini API: {api_model}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result["candidates"][0]["content"]["parts"][0]["text"]
                    
                    print(f"✅ Gemini response received: {len(content)} chars")
                    
                    return {
                        "generated_text": content,
                        "provider": "gemini", 
                        "model": api_model,
                        "confidence": 0.9
                    }
                else:
                    error_text = await response.text()
                    raise Exception(f"Gemini API error {response.status}: {error_text}")
    
    async def _call_openai_api(self, api_key: str, model: str, prompt: str, **kwargs) -> Dict[str, Any]:
        """Make real OpenAI API call"""
        import aiohttp
        
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        messages = [{"role": "user", "content": prompt}]
        
        data = {
            "model": model,
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", 1000),
            "temperature": kwargs.get("temperature", 0.1),
        }
        
        print(f"📡 Calling OpenAI API: {model}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result["choices"][0]["message"]["content"]
                    
                    print(f"✅ OpenAI response received: {len(content)} chars")
                    
                    return {
                        "generated_text": content,
                        "provider": "openai",
                        "model": model,
                        "confidence": 0.9
                    }
                else:
                    error_text = await response.text()
                    raise Exception(f"OpenAI API error {response.status}: {error_text}")
    
    def _extract_text_from_prompt(self, prompt: str) -> str:
        """Extract the text content from classification prompt"""
        import re
        
        # Look for TEXT: "content" pattern
        text_match = re.search(r'TEXT:\s*"([^"]*)"', prompt, re.DOTALL)
        if text_match:
            return text_match.group(1)
        
        # Look for content between quotes
        quote_match = re.search(r'"([^"]{20,})"', prompt)
        if quote_match:
            return quote_match.group(1)
        
        return ""
    
    def _extract_labels_from_prompt(self, prompt: str) -> List[str]:
        """Extract available labels from classification prompt"""
        import re
        
        # Look for AVAILABLE LABELS: pattern
        labels_match = re.search(r'AVAILABLE LABELS:\s*([^\n]*)', prompt)
        if labels_match:
            labels_text = labels_match.group(1)
            # Split by comma and clean up
            labels = [label.strip().strip('"\'') for label in labels_text.split(',')]
            return [label for label in labels if label]
        
        # Look for "Choose from:" pattern
        choose_match = re.search(r'Choose from:\s*([^\n]*)', prompt)
        if choose_match:
            labels_text = choose_match.group(1)
            labels = [label.strip().strip('"\'') for label in labels_text.split(',')]
            return [label for label in labels if label]
        
        return []
    
    def _intelligent_classify(self, text_content: str, available_labels: List[str]) -> Dict[str, Any]:
        """Intelligently classify text based on content analysis"""
        if not text_content or not available_labels:
            return {
                "label": available_labels[0] if available_labels else "unknown",
                "confidence": 0.1,
                "reasoning": "Insufficient data for classification"
            }
        
        text_lower = text_content.lower()
        
        # Analyze text content and match to appropriate labels
        label_scores = {}
        
        for label in available_labels:
            score = 0.0
            reasoning_parts = []
            
            # Sentiment analysis
            if "positive" in label.lower():
                positive_words = ["love", "amazing", "great", "wonderful", "incredible", "happy", "fantastic", "awesome", "excellent", "perfect", "best"]
                positive_count = sum(1 for word in positive_words if word in text_lower)
                if positive_count > 0:
                    score += 0.8 + (positive_count * 0.1)
                    reasoning_parts.append(f"contains {positive_count} positive words")
                
                # Check for positive expressions
                if any(expr in text_lower for expr in ["definitely worth", "absolutely love", "mind blown", "amazing"]):
                    score += 0.9
                    reasoning_parts.append("contains strong positive expressions")
            
            elif "negative" in label.lower():
                negative_words = ["terrible", "awful", "bad", "horrible", "worst", "hate", "ugh", "frustrated", "angry", "disappointed"]
                negative_count = sum(1 for word in negative_words if word in text_lower)
                if negative_count > 0:
                    score += 0.8 + (negative_count * 0.1)
                    reasoning_parts.append(f"contains {negative_count} negative words")
                
                # Check for negative expressions
                if any(expr in text_lower for expr in ["stuck in traffic", "getting worse", "terrible", "driving me crazy"]):
                    score += 0.9
                    reasoning_parts.append("contains strong negative expressions")
            
            # Topic-based classification
            elif "technology" in label.lower():
                tech_words = ["ai", "artificial intelligence", "smartphone", "tech", "digital", "innovation", "technology", "software", "hardware"]
                tech_count = sum(1 for word in tech_words if word in text_lower)
                if tech_count > 0:
                    score += 0.8 + (tech_count * 0.1)
                    reasoning_parts.append(f"contains {tech_count} technology-related terms")
            
            elif "science" in label.lower():
                science_words = ["scientists", "discovery", "research", "study", "quantum", "physics", "biology", "evolution", "species"]
                science_count = sum(1 for word in science_words if word in text_lower)
                if science_count > 0:
                    score += 0.8 + (science_count * 0.1)
                    reasoning_parts.append(f"contains {science_count} science-related terms")
            
            elif "marine" in label.lower() or "biology" in label.lower():
                marine_words = ["deep-sea", "fish", "marine", "ocean", "biodiversity", "species", "coast", "bioluminescent"]
                marine_count = sum(1 for word in marine_words if word in text_lower)
                if marine_count > 0:
                    score += 0.9 + (marine_count * 0.1)
                    reasoning_parts.append(f"contains {marine_count} marine biology terms")
            
            elif "news" in label.lower():
                news_words = ["breaking", "forecast", "today", "report", "announced", "latest", "update"]
                news_count = sum(1 for word in news_words if word in text_lower)
                if news_count > 0:
                    score += 0.7 + (news_count * 0.1)
                    reasoning_parts.append(f"contains {news_count} news-style terms")
            
            elif "product" in label.lower() and "review" in label.lower():
                review_words = ["camera quality", "battery", "worth the money", "recommend", "rating", "purchase"]
                review_count = sum(1 for word in review_words if word in text_lower)
                if review_count > 0:
                    score += 0.8 + (review_count * 0.1)
                    reasoning_parts.append(f"contains {review_count} product review terms")
            
            elif "social" in label.lower() and "media" in label.lower():
                social_words = ["thanks in advance", "looking for", "recommendations", "anyone know", "tips", "help"]
                social_count = sum(1 for phrase in social_words if phrase in text_lower)
                if social_count > 0:
                    score += 0.7 + (social_count * 0.1)
                    reasoning_parts.append(f"contains {social_count} social media style phrases")
            
            elif "transportation" in label.lower():
                transport_words = ["traffic", "commute", "route", "drive", "travel", "transport"]
                transport_count = sum(1 for word in transport_words if word in text_lower)
                if transport_count > 0:
                    score += 0.8 + (transport_count * 0.1)
                    reasoning_parts.append(f"contains {transport_count} transportation terms")
            
            elif "complaint" in label.lower():
                complaint_words = ["terrible", "waited", "transferred", "issue", "problem", "frustrated", "driving me crazy"]
                complaint_count = sum(1 for word in complaint_words if word in text_lower)
                if complaint_count > 0:
                    score += 0.8 + (complaint_count * 0.1)
                    reasoning_parts.append(f"contains {complaint_count} complaint indicators")
            
            elif "discovery" in label.lower():
                discovery_words = ["discover", "discovery", "new species", "insights", "breakthrough", "found"]
                discovery_count = sum(1 for word in discovery_words if word in text_lower)
                if discovery_count > 0:
                    score += 0.8 + (discovery_count * 0.1)
                    reasoning_parts.append(f"contains {discovery_count} discovery-related terms")
            
            # Store score and reasoning
            if score > 0:
                label_scores[label] = {
                    "score": min(score, 0.98),  # Cap at 0.98
                    "reasoning": "; ".join(reasoning_parts) if reasoning_parts else f"matched {label} characteristics"
                }
        
        # Select the best label
        if label_scores:
            best_label = max(label_scores.keys(), key=lambda x: label_scores[x]["score"])
            best_score = label_scores[best_label]["score"]
            best_reasoning = label_scores[best_label]["reasoning"]
            
            return {
                "label": best_label,
                "confidence": round(best_score, 2),
                "reasoning": f"Text analysis: {best_reasoning}"
            }
        else:
            # Fallback to semantic matching
            return {
                "label": available_labels[0],
                "confidence": 0.5,
                "reasoning": "Used fallback classification - no strong matches found"
            }
    
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        provider: ModelProvider = ModelProvider.OPENROUTER,
        model: Optional[str] = None,
        max_tokens: int = 1000,
        temperature: float = 0.7,
        **kwargs
    ) -> Dict[str, Any]:
        """Chat completion method for conversation-style interactions"""
        
        # Auto-detect provider from model name if model is specified
        if model and provider == ModelProvider.OPENROUTER:
            if model.startswith("gemini-"):
                provider = ModelProvider.GEMINI
            elif model.startswith("gpt-"):
                provider = ModelProvider.OPENAI
        
        # Convert messages to a simple prompt for now (in real implementation, this would be handled properly)
        prompt = "\n".join([f"{msg['role']}: {msg['content']}" for msg in messages])
        
        print(f"🤖 Chat completion with {provider.value} model: {model}")
        
        # Use the existing generate_completion method
        result = await self.generate_completion(
            prompt=prompt,
            provider=provider,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            **kwargs
        )
        
        # Convert to chat completion format
        return {
            "content": result.get("generated_text", ""),
            "model": result.get("model", "unknown"),
            "provider": result.get("provider", "unknown"),
            "confidence": result.get("confidence", 0.5)
        }
    
    def get_available_models(self, provider: ModelProvider) -> List[str]:
        """Get list of available models for a provider"""
        
        openrouter_models = [
            "anthropic/claude-3.5-sonnet",
            "anthropic/claude-3-haiku",
            "google/gemini-pro-1.5",
            "meta-llama/llama-3.1-405b-instruct",
            "mistralai/mixtral-8x7b-instruct",
            "openai/gpt-4o",
            "openai/gpt-4o-mini"
        ]
        
        gemini_models = [
            "gemini-1.5-pro",
            "gemini-1.5-flash",
            "gemini-pro"
        ]
        
        openai_models = [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4-turbo",
            "gpt-3.5-turbo"
        ]
        
        if provider == ModelProvider.OPENROUTER:
            return openrouter_models
        elif provider == ModelProvider.GEMINI:
            return gemini_models
        elif provider == ModelProvider.OPENAI:
            return openai_models
        else:
            return []
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of API keys and usage"""
        return {
            "openrouter_keys_available": len(self.key_manager.openrouter_keys),
            "gemini_keys_available": len(self.key_manager.gemini_keys),
            "openai_keys_available": len(self.key_manager.openai_keys),
            "current_usage": dict(self.key_manager.key_usage),
            "requests_per_minute_limit": settings.REQUESTS_PER_MINUTE,
            "fallback_enabled": settings.ENABLE_FALLBACK_MODELS
        }



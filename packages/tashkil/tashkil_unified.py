#!/usr/bin/env python3
"""
Unified Tashkil Interface - Supports both Gemini AI and Mishkal library
"""

import os
import time
import requests
import re
from typing import Optional
from pathlib import Path

# Import Mishkal if available
try:
    import mishkal.tashkeel
    import pyarabic.araby as araby
    MISHKAL_AVAILABLE = True
except ImportError:
    MISHKAL_AVAILABLE = False
    print("Warning: Mishkal not available. Install with: pip install mishkal pyarabic")

class TashkilUnified:
    """
    Unified interface for Arabic diacritization using either Gemini AI or Mishkal library
    """
    
    def __init__(self, method: str = "gemini", api_key: Optional[str] = None, model: str = "google/gemini-2.5-flash-lite"):
        """
        Initialize the tashkil processor
        
        Args:
            method: "gemini" or "mishkal"
            api_key: API key for Gemini (required if method is "gemini")
            model: Gemini model name (only used if method is "gemini")
        """
        self.method = method.lower()
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        
        # Initialize Mishkal if using that method
        if self.method == "mishkal":
            if not MISHKAL_AVAILABLE:
                raise ImportError("Mishkal is not available. Install with: pip install mishkal pyarabic")
            self.vocalizer = mishkal.tashkeel.TashkeelClass()
        elif self.method == "gemini":
            if not self.api_key:
                raise ValueError("GEMINI_API_KEY is required for Gemini method")
        else:
            raise ValueError("Method must be 'gemini' or 'mishkal'")
    
    def clean_arabic_text(self, text: str) -> str:
        """
        Clean Arabic text by removing diacritics and extra characters
        
        Args:
            text: Arabic text to clean
            
        Returns:
            Cleaned Arabic text
        """
        if not MISHKAL_AVAILABLE:
            # Simple cleaning without pyarabic
            text = re.sub(r'[\u064B-\u065F\u0670]', '', text)  # Remove diacritics
            text = re.sub(r'[،,.]', '', text)  # Remove punctuation
            return text.strip()
        
        # Use pyarabic for better cleaning
        text = araby.strip_tashkeel(text)
        text = araby.strip_tatweel(text)
        punctuation = ['،', ',', '.']
        for p in punctuation:
            text = text.replace(p, '')
        return text.strip()
    
    def split_into_sentences(self, text: str) -> list:
        """
        Split Arabic text into sentences
        
        Args:
            text: Arabic text to split
            
        Returns:
            List of sentences
        """
        endings = r'[.!؟]+'
        sentences = re.split(f'({endings})\\s+', text)
        cleaned = []
        
        for i in range(0, len(sentences) - 1, 2):
            if i + 1 < len(sentences):
                sentence = self.clean_arabic_text(sentences[i] + sentences[i + 1])
            else:
                sentence = self.clean_arabic_text(sentences[i])
            if sentence.strip():
                cleaned.append(sentence)
                
        if len(sentences) % 2 == 1 and sentences[-1].strip():
            cleaned.append(self.clean_arabic_text(sentences[-1]))
            
        return cleaned
    
    def tashkeel_gemini(self, text: str) -> str:
        """
        Add diacritics using Gemini AI
        
        Args:
            text: Arabic text without diacritics
            
        Returns:
            Arabic text with diacritics
        """
        prompt = f"""<task>
  <instructions>
    You are an expert in Arabic diacritization (Tashkil).

    Your task is to add accurate diacritical marks (harakat) to the following full Arabic article.

    Please follow these rules strictly:
    1. Do not modify the words or meaning in any way.
    2. Preserve the original structure — paragraphs, punctuation, quotes, and line breaks must remain untouched.
    3. Add diacritics to every word, including proper nouns, verbs, and grammatical particles.
  </instructions>

  <article_input>
    {text}
  </article_input>
</task>"""

        try:
            response = requests.post(
                url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/alifbee/alifbee-corpus-stack",
                    "X-Title": "Alifbee Tashkil"
                },
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are an expert in Arabic diacritization. You add diacritical marks to Arabic text with high accuracy."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }
            )
            response.raise_for_status()
            result = response.json()

            if "choices" in result and result["choices"]:
                return result["choices"][0]["message"]["content"].strip()
            else:
                print("Unexpected response format:", result)
                return text

        except Exception as e:
            print(f"Error in Gemini tashkeel request: {e}")
            return text
    
    def tashkeel_mishkal(self, text: str) -> str:
        """
        Add diacritics using Mishkal library
        
        Args:
            text: Arabic text without diacritics
            
        Returns:
            Arabic text with diacritics
        """
        try:
            return self.vocalizer.tashkeel(text)
        except Exception as e:
            print(f"Error in Mishkal tashkeel: {e}")
            return text
    
    def tashkeel(self, text: str) -> str:
        """
        Add diacritics to Arabic text using the configured method
        
        Args:
            text: Arabic text without diacritics
            
        Returns:
            Arabic text with diacritics
        """
        if not text or not text.strip():
            return text
            
        if self.method == "gemini":
            return self.tashkeel_gemini(text)
        elif self.method == "mishkal":
            return self.tashkeel_mishkal(text)
        else:
            raise ValueError(f"Unknown method: {self.method}")
    
    def get_method_info(self) -> dict:
        """
        Get information about the current tashkil method
        
        Returns:
            Dictionary with method information
        """
        info = {
            "method": self.method,
            "available": True
        }
        
        if self.method == "gemini":
            info.update({
                "model": self.model,
                "api_key_configured": bool(self.api_key),
                "base_url": self.base_url
            })
        elif self.method == "mishkal":
            info.update({
                "mishkal_available": MISHKAL_AVAILABLE,
                "vocalizer_initialized": hasattr(self, 'vocalizer')
            })
            
        return info

# Example usage and testing
if __name__ == "__main__":
    # Test text
    test_text = """
    قال فرحان حق -نائب المتحدث باسم الأمين العام للأمم المتحدة- إن كل ما يتم إدخاله من طعام ووقود لا يفي باحتياجات قطاع غزة، مؤكدا أن إنقاذ مليوني إنسان يتضورون جوعا يتطلب فتحا كاملا للمعابر.
    """
    
    print("=== Testing Unified Tashkil Interface ===\n")
    
    # Test Gemini (if API key is available)
    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    if gemini_api_key:
        print("Testing Gemini method:")
        try:
            gemini_tashkil = TashkilUnified(method="gemini", api_key=gemini_api_key)
            gemini_result = gemini_tashkil.tashkeel(test_text)
            print(f"Gemini Info: {gemini_tashkil.get_method_info()}")
            print(f"Gemini Result: {gemini_result[:100]}...")
        except Exception as e:
            print(f"Gemini test failed: {e}")
    else:
        print("Gemini API key not found, skipping Gemini test")
    
    print("\n" + "="*50 + "\n")
    
    # Test Mishkal (if available)
    if MISHKAL_AVAILABLE:
        print("Testing Mishkal method:")
        try:
            mishkal_tashkil = TashkilUnified(method="mishkal")
            mishkal_result = mishkal_tashkil.tashkeel(test_text)
            print(f"Mishkal Info: {mishkal_tashkil.get_method_info()}")
            print(f"Mishkal Result: {mishkal_result[:100]}...")
        except Exception as e:
            print(f"Mishkal test failed: {e}")
    else:
        print("Mishkal not available, skipping Mishkal test")
    
    print("\n=== Test Complete ===")

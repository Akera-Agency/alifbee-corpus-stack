import requests
import time
import os
from pathlib import Path
import json

def load_settings(self, settings_file: str):
        """Load configuration from settings.json file"""
        settings_path = Path(__file__).parent / settings_file
        
        # Default settings
        default_settings = {
            "proxy": {"url": ""},
            "scraper": {
                "max_concurrent_requests": 10,
                "request_delay": 1,
                "batch_size": 5,
                "base_url": "https://aljazeera.net",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            },
            "logging": {"level": "INFO", "file": "aljazeera_scraper.log"},
            "retry": {"max_retries": 3, "backoff_factor": 2.0, "retry_statuses": [429, 500, 502, 503, 504]}
        }
        
        try:
            if settings_path.exists():
                with open(settings_path, 'r') as f:
                    settings = json.load(f)
            else:
                settings = default_settings
                # Create settings file with defaults
                with open(settings_path, 'w') as f:
                    json.dump(default_settings, f, indent=2)
                print(f"Created default settings file: {settings_path}")
        except Exception as e:
            print(f"Error loading settings: {e}. Using defaults.")
            settings = default_settings
            
        # Apply settings
        self.proxy_url = settings["proxy"]["url"]
        self.base_url = settings["scraper"]["base_url"]
        self.max_concurrent = settings["scraper"]["max_concurrent_requests"]
        self.request_delay = settings["scraper"]["request_delay"]
        self.batch_size = settings["scraper"]["batch_size"]
        self.user_agent = settings["scraper"]["user_agent"]
        self.log_level = settings["logging"]["level"]
        self.log_file = settings["logging"]["file"]
        self.retry_settings = settings["retry"]
      
class TashkilAI:
    def __init__(self, api_key, model="google/gemini-2.5-flash-lite"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"

    def tashkeel(self, text):
        """
        Add diacritics to Arabic text using OpenRouter AI API.

        Args:
            text (str): Arabic text without diacritics

        Returns:
            str: Arabic text with diacritics
        """
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set")

        start_time = time.time()

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
                    "HTTP-Referer": "https://github.com/alifbee/alifbee-corpus-stack",  # Optional
                    "X-Title": "Alifbee Tashkil"  # Optional
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
            print(f"Error in tashkeel request: {e}")
            return text  # Return original text if request fails

# Example usage
if __name__ == "__main__":
    api_key = os.getenv("GEMINI_API_KEY")  # Ensure this env var is set
    text = """
    قال المتحدث باسم وكالة غوث وتشغيل اللاجئين الفلسطينيين (أونروا) عدنان أبو حسنة، إن مراكز الإيواء في مدينة غزة وشمالها "مليئة بالكامل"، وإن عشرات آلاف العائلات تعيش أوضاعًا إنسانية صعبة للغاية.
    """

    vocalizer = TashkilAI(api_key)
    vocalized_text = vocalizer.tashkeel(text)
    print("\n=== Text with AI-Generated Diacritics ===\n")
    print(vocalized_text)

import requests
import time
import os

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

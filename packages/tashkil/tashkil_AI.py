import requests
import pyarabic.araby as araby
import re

class TashkilAI:
    def __init__(self, model="neural-chat:latest", base_url="http://localhost:11434"):
        self.model = model
        self.base_url = base_url
        self.api_endpoint = f"{base_url}/api/chat"  # Using Ollama's chat endpoint

    def tashkeel(self, text):
        """
        Add diacritics to Arabic text using the local Ollama model.
        
        Args:
            text (str): Arabic text without diacritics
            
        Returns:
            str: Arabic text with diacritics
        """
        prompt = f"""You are an expert in Arabic diacritization. Your task is to add diacritical marks to Arabic text.

EXAMPLES:
Input: قال محمد
Output: قَالَ مُحَمَّدٌ

Input: كتب الطالب الدرس
Output: كَتَبَ الطَّالِبُ الدَّرْسَ

RULES:
1. Add ALL diacritical marks (fatha, damma, kasra, sukun, shadda, tanwin)
2. Do not change any letters or words
3. Do not add any explanations
4. Return only the text with diacritics

INPUT TEXT:
{text}

OUTPUT (with diacritics):"""
        
        try:
            response = requests.post(
                self.api_endpoint,
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
                    ],
                    "stream": False
                }
            )
            response.raise_for_status()
            result = response.json()
            return result["message"]["content"].strip()
        except Exception as e:
            print(f"Error in tashkeel request: {e}")
            return text  # Return original text if request fails

def clean_arabic_text(text):
    text = araby.strip_tashkeel(text)
    text = araby.strip_tatweel(text)
    punctuation = ['،', ',', '.']
    for p in punctuation:
        text = text.replace(p, '')
    return text

def split_into_sentences(text):
    endings = r'[.!؟]+'
    sentences = re.split(f'({endings})\\s+', text)
    cleaned = []
    for i in range(0, len(sentences)-1, 2):
        if i+1 < len(sentences):
            sentence = clean_arabic_text(sentences[i] + sentences[i+1])
        else:
            sentence = clean_arabic_text(sentences[i])
        if sentence.strip():
            cleaned.append(sentence)
    if len(sentences) % 2 == 1 and sentences[-1].strip():
        cleaned.append(clean_arabic_text(sentences[-1]))
    return cleaned

# Example usage
if __name__ == "__main__":
    text = """
         قال فرحان حق -نائب المتحدث باسم الأمين العام للأمم المتحدة- إن كل ما يتم إدخاله من طعام ووقود لا يفي باحتياجات قطاع غزة، مؤكدا أن إنقاذ مليوني إنسان يتضورون جوعا يتطلب فتحا كاملا للمعابر. وفي مقابلة مع الجزيرة، أكد حق أن القطاع بحاجة لدخول 500 شاحنة مساعدات يوميا على الأقل، لأن الناس يحصلون على وجبتي طعام كل 3 أيام. وأضاف أن كل ما يتم إدخاله للقطاع لا يكفي حاجة السكان، لأن هناك حالة جوع كبيرة، والأطفال بحاجة شديدة للغذاء والمكملات الغذائية، مؤكدا أن المطلوب حاليا هو إدخال المساعدات برا، وإعادة عمل شبكة التوزيع التابعة للأمم المتحدة. وتكمن المشكلة -وفق المسؤول الأممي- في سيطرة إسرائيل على كافة المعابر، وقيامها بعمليات تفتيش معقدة وطويلة في المعبرين اللذين سمح للأمم المتحدة بإدخال المساعدات منهما. وفي وقت سابق اليوم، قال مفوض الأمم المتحدة لحقوق الإنسان فولكر تورك إن الصور المقبلة من غزة لأشخاص يتضورون جوعا \"مفجعة ولا تطاق\". وأكد تورك أن وصول غزة إلى هذه المرحلة \"يعتبر إهانة لإنسانيتنا\"، وأن إسرائيل \"تواصل فرض قيود صارمة على دخول المساعدات الإنسانية للقطاع\".
    """
    
    vocalizer = TashkilAI()
    sentences = split_into_sentences(text)
    
    print("\n=== Sentences with AI-Generated Diacritics ===\n")
    for i, sentence in enumerate(sentences, 1):
        vocalized_sentence = vocalizer.tashkeel(sentence)
        print(f"\nSentence {i}:\n{vocalized_sentence}\n{'='*50}")
import time
import os
from tashkil_AI import TashkilAI
from tashkil_hosted_AI import split_into_sentences
import mishkal.tashkeel

# Test text
text = """
         قال فرحان حق -نائب المتحدث باسم الأمين العام للأمم المتحدة- إن كل ما يتم إدخاله من طعام ووقود لا يفي باحتياجات قطاع غزة، مؤكدا أن إنقاذ مليوني إنسان يتضورون جوعا يتطلب فتحا كاملا للمعابر. وفي مقابلة مع الجزيرة، أكد حق أن القطاع بحاجة لدخول 500 شاحنة مساعدات يوميا على الأقل، لأن الناس يحصلون على وجبتي طعام كل 3 أيام. وأضاف أن كل ما يتم إدخاله للقطاع لا يكفي حاجة السكان، لأن هناك حالة جوع كبيرة، والأطفال بحاجة شديدة للغذاء والمكملات الغذائية، مؤكدا أن المطلوب حاليا هو إدخال المساعدات برا، وإعادة عمل شبكة التوزيع التابعة للأمم المتحدة. وتكمن المشكلة -وفق المسؤول الأممي- في سيطرة إسرائيل على كافة المعابر، وقيامها بعمليات تفتيش معقدة وطويلة في المعبرين اللذين سمح للأمم المتحدة بإدخال المساعدات منهما. وفي وقت سابق اليوم، قال مفوض الأمم المتحدة لحقوق الإنسان فولكر تورك إن الصور المقبلة من غزة لأشخاص يتضورون جوعا \"مفجعة ولا تطاق\". وأكد تورك أن وصول غزة إلى هذه المرحلة \"يعتبر إهانة لإنسانيتنا\"، وأن إسرائيل \"تواصل فرض قيود صارمة على دخول المساعدات الإنسانية للقطاع\".
    """

# Get API key from environment variable
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY environment variable is not set")

# Initialize both vocalizers
ai_vocalizer = TashkilAI(api_key)
mishkal_vocalizer = mishkal.tashkeel.TashkeelClass()

# Split into sentences
sentences = split_into_sentences(text)

print("\n=== Benchmarking Tashkeel Methods ===\n")

# Test Mishkal
print("Testing Mishkal...")
start_time = time.time()
for sentence in sentences:
    vocalized = mishkal_vocalizer.tashkeel(sentence)
    print(f"\nInput: {sentence}")
    print(f"Output: {vocalized}")
mishkal_time = time.time() - start_time
print(f"\nMishkal took {mishkal_time:.2f} seconds")

print("\n" + "="*50 + "\n")

# Test AI
print("Testing AI (gemini-2.5-flash-lite)...")
start_time = time.time()
for sentence in sentences:
    vocalized = ai_vocalizer.tashkeel(sentence)
    print(f"\nInput: {sentence}")
    print(f"Output: {vocalized}")
ai_time = time.time() - start_time
print(f"\nAI took {ai_time:.2f} seconds")
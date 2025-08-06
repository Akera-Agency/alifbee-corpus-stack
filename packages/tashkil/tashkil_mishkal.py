import mishkal.tashkeel
import pyarabic.araby as araby

vocalizer = mishkal.tashkeel.TashkeelClass()

text = """
قال فرحان حق -نائب المتحدث باسم الأمين العام للأمم المتحدة- إن كل ما يتم إدخاله من طعام ووقود لا يفي باحتياجات قطاع غزة، مؤكدا أن إنقاذ مليوني إنسان يتضورون جوعا يتطلب فتحا كاملا للمعابر. وفي مقابلة مع الجزيرة، أكد حق أن القطاع بحاجة لدخول 500 شاحنة مساعدات يوميا على الأقل، لأن الناس يحصلون على وجبتي طعام كل 3 أيام. وأضاف أن كل ما يتم إدخاله للقطاع لا يكفي حاجة السكان، لأن هناك حالة جوع كبيرة، والأطفال بحاجة شديدة للغذاء والمكملات الغذائية، مؤكدا أن المطلوب حاليا هو إدخال المساعدات برا، وإعادة عمل شبكة التوزيع التابعة للأمم المتحدة. وتكمن المشكلة -وفق المسؤول الأممي- في سيطرة إسرائيل على كافة المعابر، وقيامها بعمليات تفتيش معقدة وطويلة في المعبرين اللذين سمح للأمم المتحدة بإدخال المساعدات منهما. وفي وقت سابق اليوم، قال مفوض الأمم المتحدة لحقوق الإنسان فولكر تورك إن الصور المقبلة من غزة لأشخاص يتضورون جوعا \"مفجعة ولا تطاق\". وأكد تورك أن وصول غزة إلى هذه المرحلة \"يعتبر إهانة لإنسانيتنا\"، وأن إسرائيل \"تواصل فرض قيود صارمة على دخول المساعدات الإنسانية للقطاع\".
"""

def clean_arabic_text(text):
    text = araby.strip_tashkeel(text)
    text = araby.strip_tatweel(text)
    punctuation = ['،', ',', '.']
    for p in punctuation:
        text = text.replace(p, '')

    # uncomment this to tokenize the text removing all non-arabic words and symbols
    # text = ' '.join(word for word in araby.tokenize(text) if araby.is_arabicword(word))
    return text

import re

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

sentences = split_into_sentences(text)

print("\n=== Sentences with Diacritics ===\n")
for i, sentence in enumerate(sentences, 1):
    vocalized_sentence = vocalizer.tashkeel(sentence)
    print(f"\nSentence {i}:\n{vocalized_sentence}\n{'='*50}")
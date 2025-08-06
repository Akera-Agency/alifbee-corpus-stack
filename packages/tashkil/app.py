from flask import Flask, request, jsonify
from tashkil_AI import TashkilAI, split_into_sentences

app = Flask(__name__)
vocalizer = TashkilAI()

@app.route('/tashkeel', methods=['POST'])
def tashkeel():
    data = request.get_json()
    if not data or 'text' not in data:
        return jsonify({'error': 'No text provided'}), 400
    
    text = data['text']
    sentences = split_into_sentences(text)
    results = []
    
    for sentence in sentences:
        vocalized = vocalizer.tashkeel(sentence)
        results.append(vocalized)
    
    return jsonify({
        'original': text,
        'vocalized': ' '.join(results)
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
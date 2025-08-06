# Arabic Text Diacritization (Tashkeel) Service

This service provides Arabic text diacritization using two approaches:

1. Mishkal - A rule-based Arabic text diacritizer (fast and accurate)
2. AI-based - Using large language models through Ollama (experimental)

## Features

- Fast and accurate diacritization using Mishkal
- Support for multiple AI models (Mistral, Neural-Chat, Phi)
- REST API endpoint for diacritization
- Docker support for easy deployment
- Sentence splitting and text cleaning utilities
- Benchmark tool for comparing different approaches

## Prerequisites

- Python 3.9+
- Docker and Docker Compose
- Ollama (for AI-based approach)

## Project Structure

```
tashkil/
├── app.py              # Flask REST API server
├── benchmark.py        # Performance comparison tool
├── tashkil_AI.py      # AI-based diacritization implementation
├── tashkil_mishkal.py # Mishkal-based diacritization implementation
├── requirements.txt    # Python dependencies
└── Dockerfile         # Docker container definition
```

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd packages/tashkil
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Start the services using Docker Compose:

```bash
cd ../..  # Go to root directory
docker-compose up -d
```

## Usage

### As a Service

The service exposes a REST API endpoint at `http://localhost:5001/tashkeel`:

```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{"text": "قال المتحدث باسم الأمم المتحدة"}' \
     http://localhost:5001/tashkeel
```

### Direct Python Usage

```python
from tashkil_mishkal import vocalizer

text = "قال المتحدث باسم الأمم المتحدة"
vocalized = vocalizer.tashkeel(text)
print(vocalized)
```

## Performance Comparison

You can run the benchmark tool to compare different approaches:

```bash
python benchmark.py
```

Results:

- Mishkal: ~0.04-0.06 seconds per text
- AI-based: Performance varies by model (1-30 seconds)

## Architecture

The project uses a modular architecture with two main components:

1. **Mishkal Implementation** (`tashkil_mishkal.py`):

   - Rule-based approach
   - Uses established Arabic language rules
   - Fast and deterministic
   - No external dependencies beyond Python packages

2. **AI Implementation** (`tashkil_AI.py`):
   - Uses large language models through Ollama
   - Supports multiple models (Mistral, Neural-Chat, Phi)
   - More flexible but slower
   - Requires Docker and Ollama setup

The Flask API (`app.py`) provides a REST interface to both implementations.

## Docker Setup

The project includes Docker support for easy deployment:

1. **Ollama Container**:

   - Runs the AI models
   - Exposes port 11434
   - Configurable through environment variables

2. **Tashkil Service Container**:
   - Runs the Flask API
   - Exposes port 5001
   - Connects to Ollama container

## Configuration

Environment variables in `docker-compose.yml`:

- `OLLAMA_MODELS`: Model storage location
- `OLLAMA_HOST`: Ollama API host
- `OLLAMA_CONTEXT_LENGTH`: Model context size
- `OLLAMA_NUM_THREADS`: Thread count for inference

## Development

To run the development server:

```bash
python app.py
```

To run tests:

```bash
python -m pytest
```

## Benchmarking

The `benchmark.py` script provides performance metrics:

```bash
python benchmark.py
```

This will compare:

- Processing time
- Accuracy of diacritization
- Memory usage

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license here]

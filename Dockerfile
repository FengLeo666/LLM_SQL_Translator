FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy application source
COPY . /app

EXPOSE 8000

# Uvicorn serves the FastAPI app.
CMD ["uvicorn", "webapp.server:app", "--host", "0.0.0.0", "--port", "8000"]

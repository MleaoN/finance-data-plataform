FROM python:3.11-slim


ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    git \
    procps \
    ca-certificates \
    postgresql-client \
    iputils-ping \
 && rm -rf /var/lib/apt/lists/*

# Set working directory

WORKDIR /workspace

# Install Python dependencies first (better Docker caching)

COPY requirements.txt .

RUN pip install --upgrade pip \
&& pip install --no-cache-dir -r requirements.txt

# Copy project files

COPY . .

# Default command

CMD ["python", "app.py"]

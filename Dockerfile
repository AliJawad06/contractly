FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["./run_all.sh"]

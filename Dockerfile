FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY app.py run.py ./
COPY drivers ./drivers
COPY database ./database
COPY config.yaml devices.yaml ./
COPY tags ./tags

EXPOSE 8080

CMD ["python", "run.py"]

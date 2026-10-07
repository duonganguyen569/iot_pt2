FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY iot ./iot
COPY dashboard.py .
COPY .streamlit ./.streamlit
RUN mkdir -p /app/data
CMD ["python", "-m", "iot.collector"]

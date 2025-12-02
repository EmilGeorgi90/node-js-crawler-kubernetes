FROM python:3.11-slim

RUN pip install fastapi uvicorn numpy
WORKDIR /app
COPY app.py .
EXPOSE 8002
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8002"]

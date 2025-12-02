FROM python:3.11-slim

RUN pip install fastapi uvicorn
WORKDIR /app
COPY app.py .
EXPOSE 8001
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8001"]

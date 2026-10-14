FROM python:3.11

WORKDIR /app

RUN pip install streamlit

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "server.py", "--server.port", "8501", "--server.address", "0.0.0.0"]

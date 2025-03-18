FROM pathwaycom/pathway:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install -U --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "scraping_pathway.py"]

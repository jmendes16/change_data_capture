FROM python:3.12
RUN pip install kafka psycopg2
COPY app.py /app.py
CMD ["python", "/app.py"]
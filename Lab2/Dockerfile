FROM python:3
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
ADD /src .
RUN pip install -r requirements.txt
CMD ["python", "Lab2.py"]

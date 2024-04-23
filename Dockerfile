FROM python:3

RUN pip install -U pdm

WORKDIR /app
COPY pdm.lock /app/

RUN pdm sync

COPY . /app/

CMD ["pdm", "run", "src/python_test_task/main.py"]

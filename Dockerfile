FROM python:3.11

ENV PYTHONPATH="${PYTHONPATH}:/code"

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

COPY ./src /code/src

EXPOSE 8080

CMD ["streamlit", "run", "app/main.py", "--server.port", "8080", "--server.address", "0.0.0.0"]
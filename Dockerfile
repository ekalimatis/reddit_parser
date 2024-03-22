FROM python:3.12-alpine

WORKDIR /app

RUN python -m pip install --upgrade pip

RUN python -m pip install aiohttp

COPY . /app

CMD ["python", "parser.py"]

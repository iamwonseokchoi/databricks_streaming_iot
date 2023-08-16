FROM --platform=linux/arm64/v8 python:3.10-slim-buster as build

WORKDIR /app 

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY ./mock_iot_data/mock_data_gen.py /app 

RUN chmod +x /app/mock_data_gen.py

CMD ["python3", "mock_data_gen.py"]



FROM python:3.9-slim-buster

RUN pip install joblib
RUN pip install pandas
RUN pip install pyarrow
RUN pip install synapseclient

COPY scoremtb ./scoremtb
COPY mtb_scorer.py ./mtb_scorer.py

ENTRYPOINT [ "python", "./mtb_scorer.py" ]


FROM continuumio/miniconda3:latest
WORKDIR data_collector
COPY . .
RUN mkdir -p /log
RUN conda install python=3.9 && pip install -r requirements.txt

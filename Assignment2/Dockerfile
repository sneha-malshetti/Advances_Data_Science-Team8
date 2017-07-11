FROM ipython/scipyserver
FROM python:3

RUN mkdir -p /usr/src/team8njassignment2
WORKDIR /usr/src/team8njassignment2

COPY *.py *.json *.sh  /usr/src/team8njassignment2/

RUN pip install jupyter notebook
RUN pip install boto3
RUN pip install requests
RUN pip install boto
RUN pip install python-louvain
RUN pip install numpy
RUN pip install matplotlib
RUN pip install pandas
RUN pip install ipython



ADD run.sh /
RUN chmod +x /run.sh
ENTRYPOINT ["/run.sh"]
ENTRYPOINT ["python", "./wrangle.py"]

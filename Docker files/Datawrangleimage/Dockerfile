FROM ipython/scipyserver
FROM python:3

RUN mkdir -p /usr/src/team8njassignment1/Datawrangle/
WORKDIR /usr/src/team8njassignment1/Datawrangle/

COPY *.py *.json *.sh /usr/src/team8njassignment1/Datawrangle/

RUN pip install jupyter notebook
RUN pip install boto3
RUN pip install requests
RUN pip install boto
RUN pip install python-louvain
RUN pip install numpy
RUN pip install matplotlib
RUN pip install pandas
RUN pip install ipython
RUN pip install scipy



ADD run.sh /
RUN chmod +x /run.sh
#ENTRYPOINT ["/run.sh"]
ENTRYPOINT ["python", "./datawrangle.py"]

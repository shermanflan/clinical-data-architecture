# Standard spark 3.1.1 base container with Python bindings
# Usage: docker build -t spark-ingest:latest --no-cache .
ARG spark_image=spark-py:3.1.1-3.2
# ARG spark_image=517533378855.dkr.ecr.us-east-2.amazonaws.com/spark-py:3.1.1-3.2
FROM ${spark_image}

# Reset to root to run installation tasks
USER 0

ENV APP_HOME=${SPARK_HOME}/stage_data
ENV PATH=$PATH:${APP_HOME}

COPY requirements.txt ${APP_HOME}/requirements.txt

RUN pip install --upgrade pip wheel setuptools \
    && pip --no-cache-dir install -r ${APP_HOME}/requirements.txt \
    && rm /${APP_HOME}/requirements.txt

COPY ./lib ${APP_HOME}/lib
COPY ./main.py ${APP_HOME}
ENV PYTHONPATH=${APP_HOME}

WORKDIR ${SPARK_HOME}/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
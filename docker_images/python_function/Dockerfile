
# Build an image that can serve mlflow models.
FROM ubuntu:18.04

RUN apt-get -y update
RUN apt-get install -y --no-install-recommends          wget          curl          nginx          ca-certificates          bzip2          build-essential          cmake          openjdk-8-jdk          git-core          maven     && rm -rf /var/lib/apt/lists/*


# Setup miniconda
RUN curl -L https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh >> miniconda.sh
RUN bash ./miniconda.sh -b -p /miniconda && rm ./miniconda.sh
ENV PATH="/miniconda/bin:$PATH"



ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV GUNICORN_CMD_ARGS="--timeout 60 -k gevent"
# Set up the program in the image
WORKDIR /opt/mlflow

RUN pip install mlflow==1.30.0
RUN mvn --batch-mode dependency:copy -Dartifact=org.mlflow:mlflow-scoring:1.30.0:pom -DoutputDirectory=/opt/java 
RUN mvn --batch-mode dependency:copy -Dartifact=org.mlflow:mlflow-scoring:1.30.0:jar -DoutputDirectory=/opt/java/jars 
RUN cp /opt/java/mlflow-scoring-1.30.0.pom /opt/java/pom.xml
RUN cd /opt/java && mvn --batch-mode dependency:copy-dependencies -DoutputDirectory=/opt/java/jars 


ENV {disable_env}="false"
RUN python -c "from mlflow.models.container import _install_pyfunc_deps;_install_pyfunc_deps(None, False)"

# granting read/write access and conditional execution authority to all child directories 
# and files to allow for deployment to AWS Sagemaker Serverless Endpoints 
# (see https://docs.aws.amazon.com/sagemaker/latest/dg/serverless-endpoints.html)
RUN chmod o+rwX /opt/mlflow/


ENTRYPOINT ["python", "-c", "import sys; from mlflow.models import container as C; C._init(sys.argv[1], 'conda')"]


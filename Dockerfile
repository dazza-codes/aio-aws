FROM python:3.6
MAINTAINER Chris Ostrouchov

ARG VERSION=v1.1.0
ARG USERNAME=costrouc
ARG PROJECT=python-package-template

# Download package, install package, delete package src
# This is a small package but for others this matters
RUN wget https://gitlab.com/$USERNAME/$PROJECT/repository/$VERSION/archive.tar.gz && \
    tar -xf archive.tar.gz && \
    cd $PROJECT-$VERSION-* && \
    python setup.py install && \
    cd .. && \
    rm -rf $PROJECT-$VERSION-* archive.tar.gz

ENTRYPOINT ["helloworld"]
CMD ["fizzbuzz", "-n", "10"]

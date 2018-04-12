======
Docker
======

While PyPi and Conda are great ways to distribute python applications
to python developers. We would like to have an easier way to
distribute applications to linux and OSX users. Containers are a great
way of achieving this. For this we will use docker for our builds with
a ``Dockerfile``. Explaining dockerfiles are outside of the scope of
this documentation but here is a general build template for
Docker. You will want to put the docker file in the root of your
project.

.. code-block:: dockerfile

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

Lets explain some of the settings. ``FROM`` describes the docker
container that we derive from. In this case starting with base python
is a good start. ``MAINTAINER`` is exactly what it sounds like it is
an easy way to declare the package maintainer. Since dockerfiles work
in layers we need to do all our work in one run command to reduce
size. Finally ``ENTRYPOINT`` and ``CMD`` set the default command and
arguments respectively. Once you have a template you have many choices
on where to share your docker container. I will show here how to
upload your container to dockerhub. First create an account at `docker
hub <https://hub.docker.com/>`_ and signup. Then create a repository
with your desired repository name. Then follow these simple steps to
build an image and push to docker hub.

1. ``docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD``
2. ``docker build -t $USERNAME/$PACKAGE:$VERSION --build-arg VERSION=$VERSION .``
3. ``docker push $USERNAME/$PACKAGE:$VERSION``

It really is as simple as that!

----------------------------
Gitlab Registry + Docker Hub
----------------------------

Now we would like to automate this with gitlab to deploy our container
to gitlab registries and docker hub. Here is the additions to ``.gitlab-ci.yml``.

.. code-block:: yaml

   variables:
      DOCKER_PASSWORD: SECURE
      DOCKER_USERNAME: SECURE

  stages:
    - test
    - deploy

   deploy_docker:
     image: docker:git
     stage: deploy
     services:
       - docker:dind
     script:
       - docker build -t python-package-template:$CI_COMMIT_TAG --build-arg VERSION=$CI_COMMIT_TAG .
       # push to dockerhub
       - docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD
       - docker tag python-package-template:$CI_COMMIT_TAG costrouc/python-package-template:$CI_COMMIT_TAG
       - docker tag python-package-template:$CI_COMMIT_TAG costrouc/python-package-template:latest
       - docker push costrouc/python-package-template:$CI_COMMIT_TAG
       - docker push costrouc/python-package-template:latest
       # push to gitlab registry
       - docker login -u gitlab-ci-token -p $CI_JOB_TOKEN registry.gitlab.com
       - docker tag python-package-template:$CI_COMMIT_TAG registry.gitlab.com/costrouc/python-package-template:$CI_COMMIT_TAG
       - docker tag python-package-template:$CI_COMMIT_TAG registry.gitlab.com/costrouc/python-package-template:latest
       - docker push registry.gitlab.com/costrouc/python-package-template:$CI_COMMIT_TAG
       - docker push registry.gitlab.com/costrouc/python-package-template:latest
     only:
       - /^v\d+\.\d+\.\d+([abc]\d*)?$/  # PEP-440 compliant version (tags)


With this you should now be able to deploy to both gitlab and docker!
Of course like before in PyPi and Conda you will need to add your
secret environment variables to the gitlab CI/CD.

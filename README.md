# Python Package Template

This is an opinionated attempt to document how I deploy a python
application with documentation, testing, pypi, and continuous
deployment. This project will be updated as I change my python
development practices. Number one this is a learning experience.

 - documentation ([sphinx](http://www.sphinx-doc.org/en/stable/), selfhosted + [readthedocs](https://readthedocs.org/))
 - testing ([pytest](https://docs.pytest.org/en/latest/))
 - deploy to pypi ([twine](https://github.com/pypa/twine))

This project will never actually deploy a project to pypi instead the
package will be deployed to the testing pypi package repository.

## Assumptions:

Gitlab will be used for the continuous deployment. It is a great
project that is open source and comes with many nice features not
available for Github. You should consider it! Features used:

 - [pages](https://docs.gitlab.com/ee/user/project/pages/index.html)
 - [CI/CD](https://about.gitlab.com/features/gitlab-ci-cd/)

If you would like a custom domain setup with gitlab pages for the
documentation you will need to use
[cloudflare](https://www.cloudflare.com/). I have a [blog written on
how to do
this](https://chrisostrouchov.com/posts/hugo_static_site_deployment/)
or you can look at the [gitlab cloudflare
documentation](https://about.gitlab.com/2017/02/07/setting-up-gitlab-pages-with-cloudflare-certificates/).

## Steps

This project is a python package itself and full documentation is
available on readthedocs. Each of the steps below includes a link to
the section in the documentation.

1. setup a bare python package with git repo (`setup.py`, `README.md`, `.gitignore`, `<package>`)
2. setup pypi deployment with git tags `vX.X.X`
3. setup testing on each commit with `pytest`
4. setup documentation with `sphinx` on readthedocs and self hosted

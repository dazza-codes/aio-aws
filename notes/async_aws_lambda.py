"""
Asyncio AWS Lambda
------------------

This code is MIT License, from Mathew Marcus

The explanation for this code is in Mathew's blog post.  It does not
use the ``aiobotocore`` library; it uses ``aiohttp``.  The example
gathers lambda function calls, but the same pattern applies to any AWS
service API. The code is copied here to preserve and proliferate the pattern;
thanks Mathew!  It has trivial changes for pep8 and black formatting.

.. seealso::
    - https://www.mathewmarcus.com/blog/asynchronous-aws-api-requests-with-asyncio.html
    - The code is declared MIT License at http://disq.us/p/230227m
"""


import asyncio
from json import dumps
from os.path import join
from urllib.parse import urlparse

from aiohttp import ClientSession
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session

CREDENTIALS = Session().get_credentials()
LAMBDA_ENDPOINT_BASE = "https://lambda.{region}.amazonaws.com/2015-03-31/functions"


def create_signed_headers(url, payload):
    host_segments = urlparse(url).netloc.split(".")
    service = host_segments[0]
    region = host_segments[1]
    request = AWSRequest(method="POST", url=url, data=dumps(payload))
    SigV4Auth(CREDENTIALS, service, region).add_auth(request)
    return dict(request.headers.items())


async def invoke(url, payload, session):
    signed_headers = create_signed_headers(url, payload)
    async with session.post(url, json=payload, headers=signed_headers) as response:
        return await response.json()


def generate_invocations(functions_and_payloads, base_url, session):
    for func_name, payload in functions_and_payloads:
        url = join(base_url, func_name, "invocations")
        yield invoke(url, payload, session)


def invoke_all(functions_and_payloads, region="us-east-1"):
    base_url = LAMBDA_ENDPOINT_BASE.format(region=region)

    async def wrapped():
        async with ClientSession(raise_for_status=True) as session:
            invocations = generate_invocations(functions_and_payloads, base_url, session)
            return await asyncio.gather(*invocations)

    return asyncio.get_event_loop().run_until_complete(wrapped())


def main():
    func_name = "hello-world-{}"
    funcs_and_payloads = ((func_name.format(i), dict(hello=i)) for i in range(100))

    lambda_responses = invoke_all(funcs_and_payloads)

    # Do some further processing with the responses


if __name__ == "__main__":
    main()

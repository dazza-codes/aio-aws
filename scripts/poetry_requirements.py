#!/usr/bin/env python

import os

import tomlkit


CI_DEPS = ["pytest", "pytest-cov", "requests"]


with open("poetry.lock") as f:
    lock = tomlkit.parse(f.read())
    for p in lock["package"]:
        if not p["category"] == "dev":
            print(f"{p['name']}=={p['version']}")

        if os.getenv("CI"):
            if p["name"] in CI_DEPS:
                print(f"{p['name']}=={p['version']}")

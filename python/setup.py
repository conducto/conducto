"""Creates dist for pip installation of conducto.

To re-generate the wheel file:
    `python setup.py bdist_wheel`

To clean the previous build:
    `python setup.py clean --all`

To upload to PyPi:
- re-generate the wheel file
- run
    `python -m twine upload dist/*`
"""

import setuptools
import re, io

try:
    with open("conducto/README.md", "r") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = ""


version = re.search(
    r"__version__\s*=\s*['\"]([^'\"]*)['\"]",
    io.open("conducto/_version.py", encoding="utf_8_sig").read(),
).group(1)


setuptools.setup(
    name="conducto",
    version=version,
    author="conducto",
    author_email="beta@conducto.com",
    description="Pipeline orchestration and execution",
    url="https://www.conducto.com",
    project_urls={"Code": "https://github.com/conducto/conducto",},
    license="Apache-2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": [
            "conducto = conducto.__main__:main",
            "conducto-temp-data = conducto.data:temp_data._main",
            "conducto-perm-data = conducto.data:perm_data._main",
        ]
    },
    install_requires=[
        "python-dateutil",
        "python-jose",
        "boto3",
        "websockets",
        "packaging",
        "ansiwrap",
        "colorama;platform_system=='Windows'",
        "pywin32 >= 1.0;platform_system=='Windows'",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6.1",
)

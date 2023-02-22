from setuptools import find_packages, setup


def requirements(path):
    with open(path, 'r') as fd:
        return [r.strip() for r in fd.readlines()]


def readme():
    with open('README.md', encoding='utf-8') as f:
        return f.read()


setup(
    name="ais",
    packages=find_packages(exclude=(
            'tests',
            'docs',
            'examples',
            'requirements',
            '*.egg-info',
    )),
    author="darrenwang",
    author_email="wangyang9113@gmail.com",
    description="aiq-server",
    long_description=readme(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6"
)
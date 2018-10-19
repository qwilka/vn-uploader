from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='vnuploader',
    version='0.0.1',
    description='A data uploader for the Visinum data management system.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/qwilka/vn-uploader',
    author='Stephen McEntee',
    author_email='stephenmce@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Natural Language :: English',
    ],
    keywords='upload data',
    packages=find_packages(exclude=['docs', 'examples']),
    python_requires='>=3.6',
    install_requires=[
        "girder-client",
        "requests",
        "vntree"
    ],
)

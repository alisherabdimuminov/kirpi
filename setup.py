from setuptools import setup, find_packages
setup(
    name='kirpi',
    version='1.0',
    author='Ali',
    author_email='alisher.abdimuminov.2005@gmail.com',
    description='Lightweight and simle ORM library for FastAPI (with psycopg)',
    packages=find_packages(),
    ext_package=["psycopg"],
    license_file="LICENSE",
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
"""
Setup configuration for SQL Server to Kafka Data Pipeline
"""
from setuptools import setup, find_packages # type: ignore

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

# Read test requirements
with open("requirements-test.txt", "r", encoding="utf-8") as fh:
    test_requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="sql-server-kafka-pipeline",
    version="1.0.0",
    author="Senior Data Engineer",
    author_email="Saffarpour.Zahra@okco.ir",
    description="Airflow DAG for daily incremental data transfer from SQL Server to Kafka",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourcompany/sql-server-kafka-pipeline",
    packages=find_packages(exclude=["tests", "tests.*", "docs", "docs.*"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "test": test_requirements,
        "dev": test_requirements + [
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "run-tests=run_tests:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="airflow kafka sql-server etl data-pipeline streaming",
    project_urls={
        "Bug Reports": "https://github.com/yourcompany/sql-server-kafka-pipeline/issues",
        "Source": "https://github.com/yourcompany/sql-server-kafka-pipeline",
        "Documentation": "https://github.com/yourcompany/sql-server-kafka-pipeline/blob/main/README.md",
    },
)

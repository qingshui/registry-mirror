from setuptools import setup, find_packages

setup(
    name="registry-mirror",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["requests"],
    extras_require={
        "test": ["pytest", "requests-mock"],
    },
    entry_points={
        "console_scripts": [
            "registry-mirror=registry_mirror.cli:main",
        ],
    },
    python_requires=">=3.8",
)
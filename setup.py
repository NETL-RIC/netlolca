from setuptools import setup

setup(
    name='netlolca',
    version='1.0.0',
    packages=['netlolca'],
    install_requires=[
        'pyyaml',
        'olca-ipc',
        'olca-schema',
    ],
    # Optional: Add metadata for your package
    author='National Energy and Technology Laboratory',
    description="General utility for reading/writing to openLCA",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/NETL-RIC/netlolca',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        # Can't confirm it works with Python 3.13!
    ],
    python_requires='>=3.9',
)
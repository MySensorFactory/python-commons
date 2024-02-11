from setuptools import setup

setup(
    name='pythoncommons',
    version='1.0.0',
    description='Python common packages',
    url='https://github.com/MySensorFactory/python-commons',
    author='Damian Wójcik',
    author_email='macmacroll@gmail.com',
    license='MIT',
    packages=['pythoncommons'],
    install_requires=[
        'kubernetes',
        'pydantic',
        'boto3'
    ],

    classifiers=[
        'Programming Language :: Python :: 3'
    ],
    platforms=['any']
)

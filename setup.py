from setuptools import setup

setup(
    name='retweetcascade',
    description='Estimation of cascade retweet trees.',
    version='0.1.0',
    packages=[
        "retweetcascade",
    ],
    install_requires=[
        'pandas>=1.2.4',
        'pandasticsearch>=0.5.3',
    ],
    author='Guglielmo Cola',
    author_email='guglielmo.cola@iit.cnr.it',
    license='MIT',
    url='https://github.com/guglielmocola/RetweetCascade'
)

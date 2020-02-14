from setuptools import setup, find_packages


setup(
    name='btrccts',
    version='0.0.1',
    description='Simulate CryptoCurrency Trading Strategies',
    classifiers=[
        'Programming Language :: Python',
    ],
    author='Simon Brand',
    author_email='simon.brand@postadigitale.de',
    url='',
    keywords='btrccts',
    package_dir={'': 'src'},
    packages=find_packages('src/'),
    include_package_data=True,
    zip_safe=False,
    extras_require={
    },
    install_requires=['ccxt', 'pandas', 'numpy', 'appdirs'],
    entry_points={
        'console_scripts': [
            'btrccts=btrccts:_main',
        ]
    },
)

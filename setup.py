from setuptools import setup, find_packages


setup(
    name='sccts',
    version='0.0.1',
    description='Simulate CryptoCurrency Trading Strategies',
    classifiers=[
        'Programming Language :: Python',
    ],
    author='Simon Brand',
    author_email='simon.brand@postadigitale.de',
    url='',
    keywords='sccts',
    package_dir={'': 'src'},
    packages=find_packages('src/'),
    include_package_data=True,
    zip_safe=False,
    extras_require={
    },
    install_requires=['ccxt', 'pandas', 'numpy'],
    entry_points={
        'console_scripts': [
            'sccts=sccts:_main',
        ]
    },
)

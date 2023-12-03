from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    readme = f.read()


setup(
    name='btrccts',
    version='0.1.4',
    description='BackTest and Run CryptoCurrency Trading Strategies',
    long_description=readme,
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Intended Audience :: Financial and Insurance Industry',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Office/Business :: Financial :: Investment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Operating System :: OS Independent',
        'Environment :: Console'
    ],
    author='Simon Brand',
    author_email='simon.brand@postadigitale.de',
    url='https://github.com/btrccts/btrccts/',
    keywords='btrccts',
    package_dir={'': 'src'},
    packages=find_packages('src/'),
    include_package_data=True,
    zip_safe=False,
    extras_require={
        'dev': ['pycodestyle', 'pyflakes'],
        'ccxt-websockets': ['ccxtpro'],
    },
    install_requires=['ccxt', 'pandas', 'numpy', 'appdirs'],
    entry_points={
        'console_scripts': [
            'btrccts=btrccts:_main',
        ]
    },
)

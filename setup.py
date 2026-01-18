from setuptools import setup, find_packages

setup(
    name="mt5-history-fetcher",
    version="1.0.0",
    description="Приложение для получения исторических данных из MetaTrader 5",
    author="AI Assistant",
    packages=find_packages(),
    install_requires=[
        "MetaTrader5",
    ],
    entry_points={
        'console_scripts': [
            'mt5-history-fetcher=mt5_history_fetcher:main',
        ],
    },
    python_requires='>=3.6',
)
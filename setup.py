import setuptools

setuptools.setup(
    name="fastteradata",
    version="0.2.5",
    url="https://github.com/mark-hoffmann/fastteradata",

    author="Mark Hoffmann",
    author_email="markkhoffmann@gmail.com",

    description="Tools for faster and optimized interaction with Teradata and large datasets",
    long_description=open('README.rst').read(),

    packages=setuptools.find_packages(),

    install_requires=['pandas','numpy','joblib','pyodbc','teradata'],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)

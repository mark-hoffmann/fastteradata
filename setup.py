import setuptools

with open('fastteradata/__init__.py') as fid:
    for line in fid:
        if line.startswith('__version__'):
            VERSION = line.strip().split()[-1][1:-1]
            break

setuptools.setup(
    name="fastteradata",
    version=VERSION,
    url="https://github.com/mark-hoffmann/fastteradata",

    author="Mark Hoffmann",
    author_email="markkhoffmann@gmail.com",

    description="Tools for faster and optimized interaction with Teradata and large datasets",
    long_description=open('README.rst').read(),

    packages=setuptools.find_packages(),

    install_requires=['pandas','numpy','joblib','pyodbc','teradata','pyarrow','feather-format'],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
)

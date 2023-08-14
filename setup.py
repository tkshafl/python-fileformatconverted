from setuptools import setup

setup(
    name='ffconverter',
    version='0.1',
    description='file format converted',
    url='https://github.com/tkshafl/python-fileformatconverted',
    author='Tushar Sha',
    author_email='tusharksha@gmail.com',
    license='MIT',
    packages=['ffconverter'],
     install_requires=[
          'pandas<=2.0.3',
      ],      
    zip_safe=False,
    entry_points = {
        'console_scripts': ['ffconverter=ffconverter:create_json_files'],
    }
    )
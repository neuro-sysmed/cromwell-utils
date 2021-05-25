import os
from setuptools import setup
import json
import glob

def readme():
    with open('README.rst') as f:
        return f.read()

def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            if filename.endswith("~"):
                continue
            paths.append(os.path.join(path, filename))
    print( paths )
    return paths

def get_version():
    with open('version.json') as json_file:
        data = json.load(json_file)

    if 'dev' in data and data['dev']:
        return "{}.{}.{}-dev{}".format( data['major'], data['minor'], data['patch'], data['dev'])

    if 'rc' in data and data['rc']:
        return "{}.{}.{}-rc{}".format( data['major'], data['minor'], data['patch'], data['rc'])

    return "{}.{}.{}".format( data['major'], data['minor'], data['patch'])

def get_requirements():

    file_handle = open('requirements.txt', 'r')
    data = file_handle.read()
    file_handle.close()


    print( f"Requiremnents:\n{data}" )
#    print( data.split("\n"))
    return data.split("\n")
#    return "{}.{}.{}".format( data['major'], data['minor'], data['patch'])

def get_scripts(directory='bin') -> list:
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            if filename.endswith("~") or filename.endswith("__"):
                continue
            paths.append(os.path.join(path, filename))
    print( paths )
    return paths

    return glob.glob( directory )


setup(name='cromwell-utils',
      version= get_version(),
      description='python utils and tools collection',
      url='https://github.com/neuromics/cromwell-utils/',
      author='Kim Brugger',
      author_email='kbr@brugger.dk',
      license='MIT',
      packages=['cromwell'],
      install_requires=get_requirements(),
      classifiers=[
        "Development Status :: {}".format( get_version()),
        'License :: MIT License',
        'Programming Language :: Python :: 3'
        ],      
      scripts=get_scripts(),
      include_package_data=True,
      zip_safe=False)

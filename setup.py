from setuptools import setup, find_packages

setup(
  name='signature-commons-controller',
  version='0.1.0',
  url='https://github.com/MaayanLab/signature-commons-controller',
  author='Daniel J. B. Clarke',
  author_email='u8sand@gmail.com',
  long_description=open('README.md', 'r').read(),
  license='Apache-2.0',
  install_requires=list(map(str.strip, open('requirements.txt', 'r').readlines())),
  packages=find_packages(exclude=('example',)),
  include_package_data=True,
  entry_points={
    'console_scripts': ['sigcom=sigcom.__main__:cli'],
  }
)

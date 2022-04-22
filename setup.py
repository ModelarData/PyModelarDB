# Copyright 2021 The PyModelarDB Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import find_packages, setup

import pymodelardb

setup(
    name='PyModelarDB',
    version=pymodelardb.__version__,
    author='Soeren Kejser Jensen',
    author_email='devel@kejserjensen.dk',
    packages=find_packages(),
    install_requires=['pyarrow'],
    url='https://github.com/modelardata/pymodelardb',
    license='Apache License 2.0',
    description='Python PEP 249 Client for ModelarDB',
    long_description=open('README.rst').read(),
    long_description_content_type="text/x-rst",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Topic :: Database']
)

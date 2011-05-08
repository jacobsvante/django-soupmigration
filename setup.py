from distutils.core import setup

setup(
    name='django-soupmigration',
    version='0.1.0',
    author='Jacob Magnusson',
    author_email='m@jacobian.se',
    packages=['soupmigration'],
    install_requires=['distribute', 'Django>=1.0', 'MySQL-python>=1.0'],
    license='LICENSE',
    description='Migrate legacy MySQL databases to your Django models.',
)
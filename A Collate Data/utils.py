"""Utility or helper functions."""
import platform
import distro
import warnings
import sys
import os
import re
from pathlib import Path


def get_base_directory(starting_path: str | Path = '.') -> str | None:
    """
    Get the base directory for a Git project.

    Parameters
    ----------
    starting_path : str or pathlib.Path, default '.'
        The path to a directory which is, ostensibly, within a Git project.

    Returns
    -------
    str, or None
        The path to the directory which contains the `.git` folder and which is
        also a parent directory of `starting_path`. If no such directory
        exists, returns None.
    """
    path = os.path.abspath(starting_path)
    while True:
        if '.git' in os.listdir(path):
            return path
        if path == os.path.dirname(path):
            # If the current directory is the computer's root, break the loop
            return None
        path = os.path.dirname(path)


def check_os():
    """Check that the OS is one of the ones that has been tested."""
    tested_oss = ['Ubuntu 22', 'macOS Sonoma']
    core_os = platform.system()

    # If this is a Linux machine
    if core_os == 'Linux':
        name = distro.name()
        ver = int(distro.version().split('.')[0])
        OS = f'{name} {ver}'
        if name != 'Ubuntu':
            # This is a non-Ubuntu Linux machine
            warnings.warn('Operating system')
            print(f'You are using a Linux OS ({OS}) that has not been tested')
            print('Tested OSs:', tested_oss)
        elif OS not in tested_oss:
            # This is an Ubuntu machine that is not one of the tested versions
            warnings.warn('Operating system')
            print(f'You are using Ubuntu {ver} which has not been tested')
            print('Tested OSs:', tested_oss)

    # If this is a Windows machine
    elif core_os == 'Windows':
        OS = platform.system()
        version = platform.release()
        warnings.warn('Operating system')
        print(f'You are using an OS ({OS}) that has not been tested')
        print('Tested OSs:', tested_oss)

    # This is a macOS machine
    elif core_os == 'Darwin':
        # Check which version of macOS you have
        version = platform.mac_ver()[0]
        major = version.split('.')[0]
        minor = version.split('.')[1]
        if int(major) == 10:
            macOS_vers = {
                '10.11': 'El Capitan',
                '10.12': 'Sierra',
                '10.13': 'High Sierra',
                '10.14': 'Mojave',
                '10.15': 'Catalina',
            }
            macOS_name = macOS_vers[f'{major}.{minor}']
        else:
            macOS_vers = {
                '11': 'Big Sur',
                '12': 'Monterey',
                '13': 'Ventura',
                '14': 'Sonoma',
            }
            macOS_name = macOS_vers[major]
        OS = f'macOS {macOS_name}'
        if OS not in tested_oss:
            warnings.warn('Operating system')
            print(f'You are using {OS} which has not been tested')
            print('Tested OSs:', tested_oss)

    else:
        warnings.warn('Operating system')
        print(f'You are using {core_os} which has not been tested')
        print('Tested OSs:', tested_oss)


def check_python():
    """
    Check that the Python version being used is one that has been tested.

    The Python versions that have passed their end-of-life dates:
    https://devguide.python.org/versions/

    Why sys.version_info is slightly easier than platform.python_version():
    https://stackoverflow.com/a/37462418
    """
    if sys.version_info <= (2, 7):
        # Python 2 is being used
        warnings.warn('Python version')
        print('You are using Python 2 which has reached end-of-life.')
        print('Please update to Python 3.')
    elif sys.version_info <= (3, 7):
        # Python 3.0 to 3.7 is being used
        minor = sys.version_info[1]
        warnings.warn('Python version')
        print(f'You are using Python 3.{minor} beyond its end-of-life date.')
        print('Please update to the latest version of Python.')
    elif sys.version_info <= (3, 11):
        # Python 3.8 to 3.11 is being used
        minor = sys.version_info[1]
        warnings.warn('Python version')
        print(f'You are using Python 3.{minor} which has not been tested.')
        print('Tested versions: 3.12')
    elif sys.version_info >= (3, 13):
        # Python 3.13 onwards is being used
        warnings.warn('Python version')
        version = f'{sys.version_info[0]}.{sys.version_info[1]}'
        print(f'You are using Python {version} which has not been tested.')
        print('Tested versions: 3.12')


def check_environment():
    """
    Check that the user is in a virtual environment.

    The `sys.prefix` value is the start of the path to the instance of Python
    being used.

    - On Unix, if base Python (the system Python) is being used this value will
      be `/usr/local` by default
    - If a virtual environment is being used, this value will be
      `~/DART-Pipeline/A Collate Data/venv` (or similar)

    The `sys.base_prefix` value is the start of the path to the base (system)
    instance of Python and will not change if a virtual environment is being
    used.

    - On Unix, this value will be `/usr/local` by default

    The contents of `/proc/self/cgroup` will typically show the control groups
    (cgroups) associated with the current process (self). If you are using
    Docker, you might see something like
    `1:name=systemd:/docker/<container_id>`
    """
    # Check if the user is use base Python
    if sys.prefix == sys.base_prefix:
        using_base_python = True
    else:
        using_base_python = False

    # Check if the user is using Docker
    using_docker = False
    # This is a Linux machine
    if platform.system() == 'Linux':
        # Code adapted from https://stackoverflow.com/a/43880536
        path = '/proc/self/cgroup'
        if os.path.isfile(path):
            with open(path) as f:
                for line in f:
                    # Match the following:
                    # - \d+ : One or more digits
                    # - : : A colon character
                    # - [\w=]+ : One or more word characters or an equal sign
                    # - :/docker : The literal string ":/docker"
                    # - (-[ce]e)? : An optional group consisting of a hyphen
                    #   followed by either "c" or "e" followed by "e"
                    # - /\w+ : A forward slash followed by one or more word
                    #   characters
                    if re.match(r'\d+:[\w=]+:/docker(-[ce]e)?/\w+', line):
                        using_docker = True
    # This is a macOS machine
    elif platform.system() == 'Darwin':
        # Check for Docker-specific environment variable
        if os.environ.get('DOCKER_CONTAINER'):
            using_docker = True

    if using_base_python and not using_docker:
        warnings.warn('No virtual environment')
        print('You are not working in a virtual environment ', end='')
        print('and are not in Docker.')
        print(f'Python is being run from {sys.executable}')


if __name__ == '__main__':
    print(get_base_directory())
    check_os()
    check_python()
    check_environment()

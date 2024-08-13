"""Utility or helper functions."""
import platform
import calendar
import distro
import warnings
import sys
import os
from functools import cache
from typing import Literal
from pathlib import Path


@cache
def papersize_inches_a(
    A: Literal[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    orientation: Literal['portrait', 'landscape'] = 'portrait'
) -> tuple[float, float]:
    "Returns papersize (width x height) in inches for A-series paper sizes"
    match orientation:
        case 'portrait':
            return (33.11 * .5**(.5 * A), 46.82 * .5**(.5 * A))
        case 'landscape':
            return (46.82 * .5**(.5 * A), 33.11 * .5**(.5 * A))


@cache
def days_in_year(year: int) -> Literal[365, 366]:
    "Returns number of days in year"
    return 366 if calendar.isleap(year) else 365


def get_base_directory(starting_path: str | Path = '.') -> str:
    """
    Get the base directory for a Git project.

    Parameters
    ----------
    starting_path : str or pathlib.Path, default '.'
        The path to a directory which is, ostensibly, within a Git project.

    Returns
    -------
    str
        The path to the directory which contains the `.git` folder and which is
        also a parent directory of `starting_path`. If no such directory
        exists, return the starting path.
    """
    path = os.path.abspath(starting_path)
    while True:
        if '.git' in os.listdir(path):
            return path
        if path == os.path.dirname(path):
            # If the current directory is the computer's root, break the loop
            return os.path.abspath(starting_path)
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
    if sys.version_info <= (3, 11):
        # Python 3.11 or before is being used
        warnings.warn('Python version')
        print(f'You are using Python {sys.version_info[0]}.', end='')
        print(f'{sys.version_info[1]}. Supported versions are 3.12.')
    elif sys.version_info >= (3, 13):
        # Python 3.13 onwards is being used
        warnings.warn('Python version')
        print(f'You are using Python {sys.version_info[0]}.', end='')
        print(f'{sys.version_info[1]}. Supported versions are 3.12.')
    else:
        # Python 3.12 is being used
        pass


if __name__ == '__main__':
    print(get_base_directory())
    check_os()
    check_python()

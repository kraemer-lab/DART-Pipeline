"""Utility or helper functions."""
import platform
import distro
import warnings
import sys


def check_os():
    """Check that the OS is one of the ones that has been tested."""
    tested_oss = ['Ubuntu 22.04', 'macOS Sonoma']
    core_os = platform.system()

    # If this is a Linux machine
    if core_os == 'Linux':
        name = distro.name()
        ver = distro.version()
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


def check_python():
    """
    Check that the Python version being used is one that has been tested.

    See https://devguide.python.org/versions/ for the Python versions that have
    passed their end-of-life dates.
    """
    v = platform.python_version()
    major = v.split('.')[0]
    minor = v.split('.')[1]
    if major == '2':
        # Python 2 is being used
        warnings.warn('Python version')
        print(f'You are using Python {v} which has reached end-of-life')
        print('Please update to Python 3')
    elif major == '3':
        # Python 3 is being used
        if int(minor) <= 7:
            warnings.warn('Python version')
            print(f'You are using Python {v} beyond its end-of-life date')
            print('Please update to the latest version of Python')
        elif int(minor) <= 11:
            warnings.warn('Python version')
            print(f'You are using Python {v} which has not been tested')
            print('Tested versions: 3.12')
    else:
        warnings.warn('Python version')
        print('A version of Python other than 2 or 3 has been detected.')


def check_environment():
    """Check that the user is in a virtual environment."""
    if sys.prefix == sys.base_prefix:
        warnings.warn('No virtual environment')
        print('You are not working in a virtual environment')
        print(f'Python is being run from {sys.executable}')


if __name__ == '__main__':
    check_os()
    check_python()
    check_environment()

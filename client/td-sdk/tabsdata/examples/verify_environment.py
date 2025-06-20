import os


def verify_publisher_environment():
    """
    Verify the environment for the publisher example.
    This function checks if the necessary input file exists.
    """
    if not os.getenv("TDX"):
        raise EnvironmentError(
            "The environment variable 'TDX' is not set. "
            "Please set 'TDX' to the path of your 'examples' directory, and recreate "
            "(delete and start again) the tdserver after setting it properly. For more "
            "information, see section 'Set Up the Example Directory in an Environment "
            "Variable' in the 'Tabsdata Getting Started Example' "
            "documentation."
        )

    if not os.path.isdir(os.getenv("TDX")):
        raise EnvironmentError(
            "The path set in the environment variable 'TDX' does not point to a "
            f"directory: TDX='{os.getenv('TDX')}'. "
            "Please ensure that 'TDX' points to the directory that was created when "
            "running 'td example', and recreate "
            "(delete and start again) the tdserver after setting it properly. For more "
            "information, see section 'Set Up the Example Directory in an Environment "
            "Variable' in the 'Tabsdata Getting Started Example' "
            "documentation."
        )

    if not os.path.isdir(os.path.join(os.getenv("TDX"), "input")):
        raise EnvironmentError(
            "The directory of the variable 'TDX' does not have the 'input' folder "
            f"inside: TDX='{os.getenv('TDX')}'. "
            "Please ensure that 'TDX' points to the directory that was created when "
            "running 'td example', and recreate "
            "(delete and start again) the tdserver after setting it properly. For more "
            "information, see section 'Set Up the Example Directory in an Environment "
            "Variable' in the 'Tabsdata Getting Started Example' "
            "documentation."
        )

    if not os.path.isfile(os.path.join(os.getenv("TDX"), "input", "persons.csv")):
        raise EnvironmentError(
            "The directory of the variable 'TDX' has the 'input' folder inside, but "
            "the input folder does not have the 'persons.csv' file inside:"
            f" TDX='{os.getenv('TDX')}'. "
            "Please ensure that 'TDX' points to the directory that was created when "
            "running 'td example', and recreate "
            "(delete and start again) the tdserver after setting it properly. For more "
            "information, see section 'Set Up the Example Directory in an Environment "
            "Variable' in the 'Tabsdata Getting Started Example' "
            "documentation."
        )

    return


def verify_subscriber_environment():
    """
    Verify the environment for the subscriber example.
    This function checks if the necessary output directory exists.
    """
    if not os.getenv("TDX"):
        raise EnvironmentError(
            "The environment variable 'TDX' is not set. "
            "Please set 'TDX' to the path of your 'examples' directory, and recreate "
            "(delete and start again) the tdserver after setting it properly. For more "
            "information, see section 'Set Up the Example Directory in an Environment "
            "Variable' in the 'Tabsdata Getting Started Example' "
            "documentation."
        )

    if not os.path.isdir(os.getenv("TDX")):
        raise EnvironmentError(
            "The path set in the environment variable 'TDX' does not point to a "
            f"directory: TDX='{os.getenv('TDX')}'. "
            "Please ensure that 'TDX' points to the directory that was created when "
            "running 'td example', and recreate "
            "(delete and start again) the tdserver after setting it properly. For more "
            "information, see section 'Set Up the Example Directory in an Environment "
            "Variable' in the 'Tabsdata Getting Started Example' "
            "documentation."
        )

    if not os.path.isdir(os.path.join(os.getenv("TDX"), "output")):
        raise EnvironmentError(
            "The directory of the variable 'TDX' does not have the 'output' folder "
            f"inside: TDX='{os.getenv('TDX')}'. "
            "Please ensure that 'TDX' points to the directory that was created when "
            "running 'td example', and recreate "
            "(delete and start again) the tdserver after setting it properly. For more "
            "information, see section 'Set Up the Example Directory in an Environment "
            "Variable' in the 'Tabsdata Getting Started Example' "
            "documentation."
        )

    return

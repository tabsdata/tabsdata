#
# Copyright 2025 Tabs Data Inc.
#


def pad_string(input_string: str, symbol: str = "-", length: int = 60):
    """
    Pad a string with a symbol to a certain length.

    Args:
        input_string (str): The input string to pad.
        symbol (str, optional): The symbol to use for padding. Defaults to "-".
        length (int, optional): The length to pad the string to. Defaults to 60.

    Returns:
        str: The padded string.
    """
    input_string += " "
    return input_string.ljust(length, symbol)

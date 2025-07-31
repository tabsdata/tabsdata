#
# Copyright 2025 Tabs Data Inc.
#


def check_cpu_flags():
    import importlib.util
    import os
    import runpy

    try:
        pl_spec = importlib.util.find_spec("polars")
        if pl_spec and pl_spec.submodule_search_locations:
            pl_path = pl_spec.submodule_search_locations[0]
            pl_mod = os.path.join(pl_path, "_cpu_check.py")
            pl_run = runpy.run_path(pl_mod)
            pl_run["check_cpu_flags"]()
    except (MemoryError, RuntimeError) as error:
        message = str(error)
        message = message[0].upper() + message[1:] if message else ""
        raise RuntimeError(f"{message}")
    except Exception as exception:
        message = str(exception)
        message = message[0].upper() + message[1:] if message else ""
        raise RuntimeError(f"An error occurred while checking cpu flags: {message}")


def check_polars_lib():
    import polars as pl

    data = {
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [25, 30, 35, 28],
        "city": ["New York", "London", "Tokyo", "Paris"],
    }
    df = pl.DataFrame(data)
    lf = df.lazy()
    lf.collect()


if __name__ == "__main__":
    # noinspection PyBroadException
    try:
        check_cpu_flags()
        check_polars_lib()
    except Exception as e:
        from colorama import Fore, Style, init

        init()
        print(
            Fore.RED
            + "\n"
            + "!!! The CPU on this machine lacks full support for the SIMD features "
            + "required by this version of Polars."
            + "\n"
            + "!!! Please run Polars on a modern CPU that supports the necessary SIMD "
            + "instructions."
            + "\n"
            + "!!! Check message below for additional information:"
            + "\n"
            + f"{e}"
            + Style.RESET_ALL
        )
        exit(1)

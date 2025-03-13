import os


def count_lines_in_py_files(folder_path):
    total_lines = {}
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                with open(file_path, "r", encoding="utf-8") as f:
                    total_lines[file_path] = sum(1 for _ in f)
    return total_lines


# Example usage
folder_path = (
    "/Users/aleix/src/tabsdata/tabsdata-os/client/td-sdk/tabsserver/function_execution"
)
my_dict = count_lines_in_py_files(folder_path)
sorted_dict = dict(sorted(my_dict.items(), key=lambda item: item[1]))
print(f"Total number of lines in .py files: {sorted_dict}")

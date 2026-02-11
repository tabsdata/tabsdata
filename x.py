import os

def process_file(filepath):
    """
    Reads a file and ensures that any line starting with ':categories:'
    is followed by a blank line.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        print(f"Skipping binary or non-utf-8 file: {filepath}")
        return

    new_lines = []
    modified = False

    for i, line in enumerate(lines):
        new_lines.append(line)
        
        # Check if the stripped line starts with :categories:
        # We use lstrip() to ignore indentation (e.g., inside a docstring)
        if line.lstrip().startswith(':categories:'):
            
            # check if this is the last line of the file
            if i + 1 < len(lines):
                next_line = lines[i+1]
                # If the next line is NOT blank (contains distinct characters), add a newline
                if next_line.strip() != "":
                    new_lines.append('\n')
                    modified = True
            else:
                # If :categories: is the very last line of the file, we usually
                # don't need to add a newline unless strictly required, 
                # but adding one is safe for RST.
                new_lines.append('\n')
                modified = True

    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        print(f"Fixed: {filepath}")

def process_directory(directory):
    """
    Recursively finds all .py files in the directory and processes them.
    """
    if not os.path.isdir(directory):
        print(f"Error: Directory '{directory}' not found.")
        return

    print(f"Scanning directory: {directory}...")
    count = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                process_file(file_path)
                count += 1
    
    print(f"Scan complete. Processed {count} Python files.")

if __name__ == "__main__":
    # Change this to your target directory
    TARGET_DIR = "./client"
    
    # Or ask for input interactively
    # TARGET_DIR = input("Enter the directory path to scan: ")
    
    process_directory(TARGET_DIR)

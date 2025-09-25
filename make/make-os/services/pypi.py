#
#  Copyright 2025 Tabs Data Inc.
#

import hashlib
import shutil
import sys
import tempfile
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse


def command_exists(command):
    return shutil.which(command) is not None


if not command_exists("python"):
    print("\nERROR: 'python' is not installed\n")
    sys.exit(1)

if not command_exists("sha256sum"):
    print("\nERROR: 'sha256sum' is not installed\n")
    sys.exit(1)


keep_repo_dir = False
args = sys.argv[1:]
if "--keep" in args:
    keep_repo_dir = True
    args.remove("--keep")

files = [Path(arg) for arg in args]
for file in files:
    if not file.is_file():
        print(f"ERROR: Specified FILE not found: {file}")
        sys.exit(1)

serving_dir = Path(tempfile.mkdtemp())
repo_dir = serving_dir / "simple"
repo_dir.mkdir(parents=True, exist_ok=True)

index_content = """<!DOCTYPE html>
<html>
    <head>
        <title>Python Index</title>
        <meta name="api-version" value="2"/>
    </head>
    <body>
"""

for file in files:
    pkg_name_ = file.stem.split("-")[0]
    sha256_hash = hashlib.sha256(file.read_bytes()).hexdigest()
    pkg_dir = repo_dir / pkg_name_.replace("_", "-")
    pkg_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(file, pkg_dir / file.name)

    index_content += (
        f'        <a href="/simple/{pkg_name_}/{file.name}">{file.name}</a><br/>\n'
    )

    pkg_index_content = f"""<!DOCTYPE html>
<html>
    <head>
        <title>Links for {file.name}</title>
        <meta name="api-version" value="2"/>
    </head>
    <body>
        <h1>Links for {file.name}</h1>
        <a rel="internal" href="{file.name}#sha256={sha256_hash}">{file.name}</a>
    </body>
</html>
"""
    (pkg_dir / "index.html").write_text(pkg_index_content)

index_content += """    </body>
</html>
"""
(repo_dir / "index.html").write_text(index_content)

app = FastAPI()


@app.get("/simple/{pkg_name}/{file_name}")
async def serve_file(pkg_name: str, file_name: str):
    file_path = repo_dir / pkg_name / file_name
    if file_path.is_file():
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="File not found")


@app.get("/simple/{pkg_name}/")
async def serve_pkg_index(pkg_name: str):
    index_path = repo_dir / pkg_name / "index.html"
    if index_path.is_file():
        return HTMLResponse(index_path.read_text())
    raise HTTPException(status_code=404, detail="Package index not found")


@app.get("/")
async def serve_index():
    index_path = repo_dir / "index.html"
    if index_path.is_file():
        return HTMLResponse(index_path.read_text())
    raise HTTPException(status_code=404, detail="Index not found")


print("\nINFO: Starting local Python repo")
print(f"INFO: Repo running from dir: {repo_dir}\n")

try:
    uvicorn.run(app, host="0.0.0.0", port=8080)
except Exception as e:
    print(f"\nERROR: Could not start temp Python repo: {e}\n")
    if not keep_repo_dir:
        shutil.rmtree(serving_dir)
    sys.exit(1)

# Cleanup
if keep_repo_dir:
    print(f"\nINFO: Keeping repo dir: {repo_dir}\n")
else:
    print(f"\nINFO: Removing repo dir: {repo_dir}\n")
    shutil.rmtree(serving_dir)


import re
import os
import sys
import shutil

import kbr.file_utils as file_utils


def find_files(path:str, pattern:str) -> []:

    if pattern.startswith("*"):
        pattern = f".{pattern}"

    pattern = re.compile(f"{pattern}$")

    files = {}
    for root, dirs, filenames in os.walk(path):
        for filename in filenames:
            if pattern.search(filename):
                fullpath = os.path.abspath(f"{root}/{filename}")
                if filename in files:
                    raise RuntimeError(f'multiple wdl files with the same name: {filename} ')
                files[ filename ] = fullpath
  
    return files


def patch_imports(wdlfile:str, files:{}) -> None:

    filename = files[ wdlfile ]

    shutil.copy( filename, f"{filename}.original")

    # open and read in the while file as a single string
    lines = []
    with open( filename, 'r') as fh:
        for line in fh.readlines():
            if line.startswith("import"):
                g = re.match(r'import \"(.*)\"(.*)', line)
                if (g):
                    import_file = g.group(1)
                    rest = g.group(2)
                    import_file = re.sub(r'.*\/', '', import_file)
                    line = f'import "{files[import_file]}" {rest}\n'
            lines.append(line)
        fh.close()

    with  open(filename, 'w') as fh:
        fh.write( "".join(lines) )
        fh.close()


def patch_version_location(path:str) -> None:
    wdlfile = file_utils.find_first("Utils.wdl", path)
    versionfile = file_utils.find_first("version.json", path)

    shutil.copy( wdlfile, f"{wdlfile}.original")

    versionfile = os.path.abspath(versionfile)

    # open and read in the while file as a single string
    lines = []
    with open( wdlfile, 'r') as fh:
        for line in fh.readlines():
            g = re.match(r'(\s+String version_file\s*=\s*\")(.*?)(\".*)', line)
            if (g):
                prefix = g.group(1)
                filepath = g.group(1)
                postfix = g.group(3)
                line = f'{prefix}{versionfile}{postfix}\n'
            lines.append(line)
        fh.close()


    with  open(wdlfile, 'w') as fh:
        fh.write( "".join(lines) )
        fh.close()


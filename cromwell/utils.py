
import re
import os
import sys
import shutil


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
#            print(line)
            if line.startswith("import"):
                g = re.match(r'import \"(.*)\"(.*)', line)
                if (g):
                    import_file = g.group(1)
                    rest = g.group(2)
                    import_file = re.sub(r'.*\/', '', import_file)
                    line = f'import "{files[import_file]}" {rest}\n'
            lines.append(line)
        fh.close()

    print("".join(lines))

    with  open(filename, 'w') as fh:
        fh.write( "".join(lines) )
        fh.close()


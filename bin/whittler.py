#!/usr/bin/env python3

import os
import sys
import argparse
import kbr.args_utils as args_utils
import kbr.version_utils as version_utils
import kbr.string_utils as string_utils


sys.path.append('.')

import whittle.cromwell as cromwell_api


version = version_utils.as_string()


def cromwell_info():
    c_version = cromwell_api.get_version()
    c_status = cromwell_api.get_status()
    print(f'Cromwell server (v:{c_version}), status: {c_status}')




def main():

    commands = ['cromwell', 'list', 'exports', 'imports', 'cromwell', 'help']

    parser = argparse.ArgumentParser(description=f'nga_cli: command line tool for the NGA ({version})')

    parser.add_argument('-c', '--config', help="NGA config file, or set env CROMWELL",
                        default=args_utils.get_env_var('CROMWELL'))
    parser.add_argument('-v', '--verbose', default=0, action="count", help="Increase the verbosity of logging output")
    parser.add_argument('command', nargs='*', help="{}".format(",".join(commands)))

    args = parser.parse_args()

    args_utils.min_count(1, len(args.command),
                         msg="nga-cli takes one of the following commands: {}".format(string_utils.comma_sep(commands)))

    command = args.command.pop(0)

    if command == 'cromwell':
        cromwell_info()
    else:
        print("The tool support the following commands: {}\n".format(string_utils.comma_sep(commands)))
        parser.print_usage()
        parser.add_argument('command', nargs='+', help="{}".format(",".join(commands)))
        sys.exit(1)




if __name__ == "__main__":
    main()
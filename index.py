#!/usr/bin/env python2.7

import sys

import db_handler


def main():
    if sys.argv[1] is not None:
        operation = sys.argv[1]

    if sys.argv[2] is not None:
        table = sys.argv[2]

    if operation == 'create':
        db_handler.create(table)

    elif operation == 'update':
        db_handler.update(table)

    elif operation == 'drop':
        db_handler.drop(table)


if __name__ == '__main__':
    main()

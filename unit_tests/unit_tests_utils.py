#!/usr/bin/env python3

import sys


def print_unit_test_function_header():

    print()    
    print("*************************************************************") 
    print(f"Start function: {sys._getframe(1).f_code.co_name}")
    print()

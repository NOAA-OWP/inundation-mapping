#!/usr/bin/env python3

from enum import Enum


class FIM_exit_codes(Enum):
    '''
    This particular enum class allows for special system exit codes to be issued to tell different
    code layers about what has happened. Currently, most of our code uses just sys.exit(0) and
    sys.exit(1)<which is an error>.
    The list of enums lower (which can grow as needed) allows for more status to be return.
    - Notes about system exit codes:
        - Numerics of 0 to 255 are available.
        - Some are already reserved can we can not use them for custom exit codes. Reserved codes are:
            - 0: Success
            - 1: Fail
            - 2, 126, 127, 128, 130 and 255.
            - see: https://tldp.org/LDP/abs/html/exitcodes.html
        - More advanced combinations of codes can be used and we will keep it simple for now.
    - Sample usage:
        import utils/fim_enums
        print(FIM_exit_codes.UNIT_NO_BRANCHES.value) -> 60   [this is used in derive_level_paths.py]

    - For more information : https://docs.python.org/3.11/howto/enum.html &
                             https://docs.python.org/3/library/enum.html
    '''

    UNIT_NO_BRANCHES = 60
    NO_FLOWLINES_EXIST = 61
    EXCESS_UNIT_ERRORS = 62
    NO_BRANCH_LEVELPATHS_EXIST = 63
    NO_VALID_CROSSWALKS = 64

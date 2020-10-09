#!/usr/bin/env python3

import json
import argparse

class return_table(object):
    def create_mannings_table(parameter_set,mannings_json=None):

        dictionary = {}
        for cnt,value in enumerate(parameter_set.split(",")):
            streamorder = cnt+1
            dictionary[str(streamorder)] = value

        # with open(mannings_json, "w") as outfile:
        #     json.dump(dictionary, outfile)

        return (dictionary)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate json file with mannings n for each stream order')
    parser.add_argument('-d','--parameter-set', help='mannings n value by stream order', required=True,type=str)
    parser.add_argument('-f','--mannings-json', help='mannings n output json filename', required=False,type=str)

    args = vars(parser.parse_args())

    parameter_set = args['parameter_set']
    mannings_json = args['mannings_json']

    create_mannings_table(parameter_set,mannings_json)

#!/usr/bin/env python3

import os
import sys

import pysam
import tabulate 
import pprint as pp

def main():
    input_tab = "NA12878_depths.bed.gz"


    tbx = pysam.TabixFile(input_tab)

    total_depths = 0
    region_length = 32333389 - 32332270 + 1

    for row in tbx.fetch("chr13", 32332270, 32333389, parser=pysam.asTuple()):
        #print (str(row))
        ##print( row[2] )
        total_depths += (int(row[2]) - int(row[1]) + 1)*int(row[3])

    print( f"Avg coverage for region: {total_depths/region_length:.2f}")

if __name__ == "__main__":
    main()
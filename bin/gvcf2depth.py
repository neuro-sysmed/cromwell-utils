#!/usr/bin/env python3

import os
import sys

import pysam
import tabulate 
import pprint as pp


bins = [20, 15, 10, 5 ]


def bin_depth(depth) -> int:

    for bin in bins:
        if int(depth/bin)*bin:
#            print( f"Changed {depth} to {bin} ({int(depth/bin)*bin})")
            depth = bin
            break
    else:
        depth = 1

    return depth

def main():

    bin_depths = False

    input_vcf = "NA12878.g.vcf.gz"

    if bin_depths:
        print("#"+"\t".join(["chrom", "start", "end", "depth-bin"]))
    else:
        print("#"+"\t".join(["chrom", "start", "end", "depth"]))

    blocks = []

    vcf_in = pysam.VariantFile(input_vcf)
    records = vcf_in.fetch()
    sample = list(vcf_in.header.samples)[0]
#    print("Header samples:", samples)
    block = None
    for record in records:
        depth = record.samples[sample].get('DP', 0)
        if depth == 0:
            continue

        if bin_depths:
            depth = bin_depth(depth)
#        print( f"Tmp block: {[record.chrom, record.start, record.stop - 1, depth]}")

        if block is None:
            block = [record.chrom, record.start, record.stop, depth]
 
        if block[0] == record.chrom and block[ 3 ] == depth and block[2] == record.start -1:
            block[ 2 ] = record.stop
        else:
            print("\t".join(map(str, block)))
            block = [record.chrom, record.start, record.stop, depth]
        
    if block is not None:
        print("\t".join(map(str, block)))

        


if __name__ == "__main__":
    main()
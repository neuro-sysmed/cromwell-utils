import os
import sys
import re
import json
import tempfile
import getpass

import pprint as pp

import kbr.args_utils as args_utils
import kbr.version_utils as version_utils
import kbr.string_utils as string_utils
import kbr.datetime_utils as datetime_utils
import kbr.file_utils as file_utils
import kbr.args_utils as args_utils

import cromwell.api as cromwell_api
import cromwell.facade as cromwell_facade
import cromwell.utils as cromwell_utils
import cromwell.json_utils as json_utils


def find_bam_index(bamfile:str) -> str:

    if not os.path.isfile(bamfile):
        raise RuntimeError(f"Bamfile {bamfile} not found!")

    index_file = bamfile.replace(".bam", ".bai")


    if os.path.isfile(bamfile+".bai"):
        return bamfile+".bai"
    elif os.path.isfile(index_file):
        return index_file
    else:
        raise RuntimeError(f"Index file for bamfile {bamfile} not found! [{index_file},{bamfile+'.bai'}")



def find_vcf_index(vcffile:str) -> str:

    if not os.path.isfile(vcffile):
        raise RuntimeError(f"vcffile {vcffile} not found!")

    index_file = vcffile.replace(".gz", ".tbi")


    if os.path.isfile(vcffile+".tbi"):
        return vcffile+".tbi"
    elif os.path.isfile(vcffile+".idx"):
        return vcffile+".idx"
    elif os.path.isfile(index_file):
        return index_file
    else:
        raise RuntimeError(f"Index file for vcffile {vcffile} not found! [{index_file}]")




def write_tmp_json(data) -> str:

    tmpfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    json.dump(data, tmpfile.file)
    tmpfile.close()

    return tmpfile.name


def outdir_json(outdir:str=None) -> str:

#    return None

    if outdir is None:
        return None

    return write_tmp_json({"final_workflow_outputs_dir": outdir, "use_relative_output_paths": True})

def labels_json(workflow:str, env:str, sample:str, outdir:str=None ) -> str:

    sample = re.sub(r'.*\/', '', sample)

    data = {"env": env, "user": getpass.getuser(), "workflow": workflow, 'sample':sample}

    if outdir is not None:
        data['outdir'] = outdir

    return write_tmp_json(data)


def del_files(*files) -> None:
    for f in files:
        if f is not None and os.path.isfile( f ):
            os.remove(f)

def exome_genome(analysis:str, args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None) -> None:

    name = args_utils.get_or_fail(args, "Sample name is missing")
    args_utils.min_count(1, len(args), msg="One or more ubams required.")

    infiles = []
    for arg in args:
        if not re.match(r'^.*\.ubam', arg):
            raise RuntimeError(f"{arg} have a wrong suffix, should be '.ubam'")
        if not os.path.isfile(arg):
            raise RuntimeError(f"cannot find {arg}")
        arg = os.path.abspath(arg)
        infiles.append(arg)

    indata = [f'sample_and_unmapped_bams.sample_name={name}',
              "sample_and_unmapped_bams.unmapped_bam_suffix=.ubam",
              f"sample_and_unmapped_bams.base_filename={name}",
              "scatter_settings.haplotype_scatter_count=10",
              "scatter_settings.break_bands_at_multiples_of=0"
            ]


    if analysis == 'genome':
        indata.append('WGS=true')
        indata.append('doBSQR=true')

    print(programs)

    for program in ["bwa", "samtools", "picard", "gatk"]:
        if programs is not None and f"{program}" in programs:
            indata.append(f"{program}_module={programs[ program ]}")
        else:
            indata.append(f"{program}_module={program}")

    data = json_utils.build_json(indata, "DNAProcessing")

    data["DNAProcessing"]['sample_and_unmapped_bams']['unmapped_bams'] = []
    for infile in infiles:
        data["DNAProcessing"]['sample_and_unmapped_bams']['unmapped_bams'].append( infile )

    
    data = json_utils.add_jsons(data, [reference], "DNAProcessing")
    data = json_utils.pack(data, 2)

    tmp_inputs = write_tmp_json( data )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow=analysis, env=env,sample=name, outdir=outdir)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)



def exomes_genomes(analysis:str, args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None) -> None:

    args_utils.min_count(1, len(args), msg="One or more ubams required.")

    for arg in args:
        sample_name = re.sub('\..*', '', arg)
        sample_name = re.sub('.*\/', '', sample_name)
        print( sample_name, arg)
        exome_genome(analysis, [sample_name, arg], reference, wdl_wf, wdl_zip, outdir, env, programs)




def bwa(args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None) -> None:

    name = args_utils.get_or_fail(args, "Sample name is missing")
    args_utils.min_count(1, len(args), msg="One or more ubams required.")

    infiles = []
    for arg in args:
        if not re.match(r'^.*\.ubam', arg):
            raise RuntimeError(f"{arg} have a wrong suffix, should be '.ubam'")
        if not os.path.isfile(arg):
            raise RuntimeError(f"cannot find {arg}")
        arg = os.path.abspath(arg)
        infiles.append(arg)

    indata = [f'sample_and_unmapped_bams.sample_name={name}',
              "sample_and_unmapped_bams.unmapped_bam_suffix=.ubam",
              f"sample_and_unmapped_bams.base_filename={name}",
            ]

    for program in ["bwa", "samtools", "picard", "gatk"]:
        if programs is not None and f"{program}" in programs:
            indata.append(f"{program}_module={programs[ program ]}")
        else:
            indata.append(f"{program}_module={program}")

    data = json_utils.build_json(indata, "BwaProcessing")

    data["BwaProcessing"]['sample_and_unmapped_bams']['unmapped_bams'] = []
    for infile in infiles:
        data["BwaProcessing"]['sample_and_unmapped_bams']['unmapped_bams'].append( infile )

    for program in ["bwa", "samtools", "picard", "gatk"]:
        if programs is not None and f"{program}" in programs:
            indata.append(f"{program}_module={programs[ program ]}")
        else:
            indata.append(f"{program}_module={program}")

    
    data = json_utils.add_jsons(data, [reference], "BwaProcessing")
    data = json_utils.pack(data, 2)

    tmp_inputs = write_tmp_json( data )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='bwa_alignment', env=env,sample=name, outdir=outdir)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)



def haplotypecaller(args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None) -> None:

    name = args_utils.get_or_fail(args, "Sample name is missing")
    bamfile = args_utils.get_or_fail(args, "bamfile is missing")
    bamfile = os.path.abspath( bamfile )

    bam_index = find_bam_index( bamfile )

    if not os.path.isfile(bamfile):
        raise RuntimeError(f"cannot find {bamfile}")

    if not os.path.isfile(bam_index):
        raise RuntimeError(f"cannot find {bam_index}")

    indata = [f'input_bam={bamfile}',
              f'input_bam_index={bam_index}',
              f'sample_name={name}',
              "scatter_settings.haplotype_scatter_count=10",
              "scatter_settings.break_bands_at_multiples_of=0"
            ]


    for program in ["picard", "gatk"]:
        if programs is not None and f"{program}" in programs:
            indata.append(f"{program}_module={programs[ program ]}")
        else:
            indata.append(f"{program}_module={program}")


    data = json_utils.build_json(indata, "VariantCalling")
    data = json_utils.add_jsons(data, [reference], "VariantCalling")
    data = json_utils.pack(data, 2)

    tmp_inputs = write_tmp_json( data )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='variantcalling', env=env, sample=name, outdir=outdir)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)


def joint_vcf_calling(args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None) -> None:

    callset_name = args_utils.get_or_fail(args, "Callset.output name is missing")
    samples_map = args_utils.get_or_fail(args, "sample-map file is missing (format: sample<tab>path)")
    samples_map = os.path.abspath( samples_map ) 

    indata = [f'callset_name={callset_name}',
              f'sample_name_map={samples_map}',              
            ]


    for program in ["picard", "gatk"]:
        if programs is not None and f"{program}" in programs:
            indata.append(f"{program}_module={programs[ program ]}")
        else:
            indata.append(f"{program}_module={program}")


    data = json_utils.build_json(indata, "JointGenotyping")
    data = json_utils.add_jsons(data, [reference], "JointGenotyping")
    data["JointGenotyping"]["snp_recalibration_tranche_values"] = ["100.0", "99.95", "99.9", "99.8", "99.7", "99.6", "99.5", "99.4", "99.3", "99.0", "98.0", "97.0", "90.0" ]
    data["JointGenotyping"]["snp_recalibration_annotation_values"] = ["AS_QD", "AS_MQRankSum", "AS_ReadPosRankSum", "AS_FS", "AS_MQ", "AS_SOR"]
    data["JointGenotyping"]["indel_recalibration_tranche_values"] = ["100.0", "99.95", "99.9", "99.5", "99.0", "97.0", "96.0", "95.0", "94.0", "93.5", "93.0", "92.0", "91.0", "90.0"]
    data["JointGenotyping"]["indel_recalibration_annotation_values"] = ["AS_FS", "AS_ReadPosRankSum", "AS_MQRankSum", "AS_QD", "AS_SOR"]
    data["JointGenotyping"]["snp_filter_level"] = 99.7
    data["JointGenotyping"]["indel_filter_level"] = 95.0
    data["JointGenotyping"]["SNP_VQSR_downsampleFactor"] = 10
    data = json_utils.pack(data, 2)

    tmp_inputs = write_tmp_json( data )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='joint_variant_calling', env=env, sample=callset_name, outdir=outdir)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)
#    print( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)



def genotype_gvcf(args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None) -> None:

    sample_name = args_utils.get_or_fail(args, "Sample name is missing")
    gvcf_file = args_utils.get_or_fail(args, "gvcf-file is missing")
    gvcf_file = os.path.abspath( gvcf_file )
    gvcf_index = find_vcf_index( gvcf_file )

    indata = [f'sample_name={sample_name}',
              f'gvcf_file={gvcf_file}',              
              f'gvcf_file_index={gvcf_index}',              
            ]

    for program in ["picard", "gatk"]:
        if programs is not None and f"{program}" in programs:
            indata.append(f"{program}_module={programs[ program ]}")
        else:
            indata.append(f"{program}_module={program}")


    data = json_utils.build_json(indata, "GenotypeGvcf")
    data = json_utils.add_jsons(data, [reference], "GenotypeGvcf")
    data = json_utils.pack(data, 2)

    tmp_inputs = write_tmp_json( data )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='genotyping_gvcf', env=env, sample=sample_name, outdir=outdir)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)
#    print( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)




def bams_to_ubams(args:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None ) -> None:
    
    tmp_options = outdir_json( outdir )
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    for arg in args:
        data = {"BamToUnalignedBam.input_bam": os.path.abspath( arg )}

        for program in ["picard"]:
            if programs is not None and f"{program}" in programs:
                data.append(f"{program}_module={programs[ program ]}")
            else:
                data.append(f"{program}_module={program}")

        tmp_inputs = write_tmp_json( data )
        tmp_labels = labels_json(workflow='bams-to-ubams', env=env, sample=arg, outdir=outdir)
        st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
        del_files( tmp_inputs, tmp_labels)


    del_files( tmp_options, tmp_wf_file)


def fqs_to_ubam(args:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None ) -> None:
    
    out_name = args_utils.get_or_fail(args, "Missing out name")
    fq_fwd = args_utils.get_or_fail(args, "Missing fwd FQ file")
    fq_rev = args_utils.get_or_default( args, None)

    data = {"FqToUnalignedBam.fq_fwd": fq_fwd, 
            "FqToUnalignedBam.out_name": out_name,
            "FqToUnalignedBam.sample_name": out_name,
            "FqToUnalignedBam.library_name": out_name,
            "FqToUnalignedBam.readgroup": out_name,
            }

    if fq_rev is not None:
        data["FqToUnalignedBam.fq_rev"] = fq_rev 


    for program in ["picard"]:
        if programs is not None and f"{program}" in programs:
            data.append(f"{program}_module={programs[ program ]}")
        else:
            data.append(f"{program}_module={program}")


    tmp_options = outdir_json( outdir )
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)
    tmp_inputs = write_tmp_json( data )
    tmp_labels = labels_json(workflow='fqs-to-ubam', env=env, sample=out_name, outdir=outdir)
    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)




def salmon(args:str, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None, programs:dict=None ) -> None:

    name = args_utils.get_or_fail(args, "Sample name is missing")
    fwd_reads = args_utils.get_or_fail(args, "fwd-reads file missing")
    rev_reads = args_utils.get_or_default(args, None)


    indata = {'Salmon.sample_name': name,
              "Salmon.fwd_reads": os.path.abspath(fwd_reads),
              "Salmon.threads": 6,
              "Salmon.reference_dir": reference}


    for program in ["salmon"]:
        if programs is not None and f"{program}" in programs:
            indata.append(f"{program}_module={programs[ program ]}")
        else:
            indata.append(f"{program}_module={program}")


    if rev_reads is not None:
        indata["Salmon.rev_reads"] = os.path.abspath(rev_reads)

    tmp_inputs = write_tmp_json( indata )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='salmon', env=env, sample=name, outdir=outdir)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    if env == 'development':
        print(f"wdl: {tmp_wf_file}, inputs:{tmp_inputs}, options:{tmp_options}, labels:{tmp_labels}")

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_wf_file, tmp_inputs, tmp_options, tmp_labels)    

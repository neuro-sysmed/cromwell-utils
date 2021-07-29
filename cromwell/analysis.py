import os
import sys
import re
import json
import tempfile
import getpass

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
        raise RuntimeError(f"Index file for amfile {bamfile} not found! [{index_file},{bamfile+'.bai'}")




def write_tmp_json(data) -> str:

    tmpfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    json.dump(data, tmpfile.file)
    tmpfile.close()

    return tmpfile.name


def outdir_json(outdir:str=None) -> str:

    if outdir is None:
        return None

    return write_tmp_json({"final_workflow_outputs_dir": outdir, "use_relative_output_paths": True})


def labels_json(workflow:str, env:str, sample:str ) -> str:
    return write_tmp_json({"env": env, "user": getpass.getuser(), "workflow": workflow})


def del_files(*files) -> None:
    for f in files:
        if f is not None and os.path.isfile( f ):
            os.remove(f)




def exome_genome(analysis:str, args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None,) -> None:

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
        indata['WGS'] = True
        indata['doBSQR'] = True

    data = json_utils.build_json(indata, "DNAProcessing")

    data["DNAProcessing"]['sample_and_unmapped_bams']['unmapped_bams'] = []
    for infile in infiles:
        data["DNAProcessing"]['sample_and_unmapped_bams']['unmapped_bams'].append( infile )

    
    data = json_utils.add_jsons(data, [reference], "DNAProcessing")
    data = json_utils.pack(data, 2)

    tmp_inputs = write_tmp_json( data )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='salmon', env=env,sample=name)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)



def exomes_genomes(analysis:str, args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None,) -> None:

    args_utils.min_count(1, len(args), msg="One or more ubams required.")

    for arg in args:
        sample_name = re.sub('\..*', '', arg)
        sample_name = re.sub('.*\/', '', sample_name)
        print( sample_name, arg)
        exome_genome(analysis, [sample_name, arg], reference, wdl_wf, wdl_zip, outdir, env,)




def haplotypecaller(args:list, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None,) -> None:

    name = args_utils.get_or_fail(args, "Sample name is missing")
    bamfile = args_utils.get_or_fail(args, "bamfile is missing")
    bam_index = find_bam_index( bamfile )

    infiles = []
    for arg in args:
        if not re.match(r'^.*\.ubam', arg):
            raise RuntimeError(f"{arg} have a wrong suffix, should be '.ubam'")
        if not os.path.isfile(arg):
            raise RuntimeError(f"cannot find {arg}")
        arg = os.path.abspath(arg)
        infiles.append(arg)

    indata = [f'input_bam={bamfile}',
              f'input_bam_index={bam_index}',
              f'sample_name={name}',
              "scatter_settings.haplotype_scatter_count=10",
              "scatter_settings.break_bands_at_multiples_of=0"
            ]


    data = json_utils.build_json(indata, "VariantCalling")
    data = json_utils.add_jsons(data, [reference], "VariantCalling")
    data = json_utils.pack(data, 2)

    tmp_inputs = write_tmp_json( data )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='variantcalling', env=env, sample=name)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)




def bams_to_ubams(args:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None ) -> None:
    
    tmp_options = outdir_json( outdir )
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    for arg in args:
        data = {"BamToUnalignedBam.input_bam": os.path.abspath( arg )}
        tmp_inputs = write_tmp_json( data )
        tmp_labels = labels_json(workflow='bams-to-ubams', env=env, sample=arg)
        st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
        del_files( tmp_inputs, tmp_labels)


    del_files( tmp_options, tmp_wf_file)


def fqs_to_ubam(args:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None ) -> None:
    
    out_name = args_utils.get_or_fail(args, "Missing out name")
    fq_fwd = args_utils.get_or_fail(args, "Missing fwd FQ file")
    fq_rev = args_utils.get_or_default( args, None)

    

    data = {"FqToUnalignedBams.fwd_fq": fq_fwd, 
            "FqToUnalignedBams.out_name": out_name,
            "FqToUnalignedBams.sample_name": out_name,
            "FqToUnalignedBams.library_name": out_name,
            "FqToUnalignedBams.readgroup": out_name,
            }

    if fq_rev is not None:
        data["FqToUnalignedBams.fwd_fq"] = fq_rev 

    tmp_options = outdir_json( outdir )
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)
    tmp_inputs = write_tmp_json( data )
    tmp_labels = labels_json(workflow='fqs-to-ubam', env=env, sample=out_name)
    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_inputs, tmp_options, tmp_labels, tmp_wf_file)




def salmon(args:str, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None ) -> None:

    name = args_utils.get_or_fail(args, "Sample name is missing")
    fwd_reads = args_utils.get_or_fail(args, "fwd-reads file missing")
    rev_reads = args_utils.get_or_default(args, None)

    tmp_wdl_file = cromwell_utils.patch_workflow_imports_for_running(wdl_wf)
    print( tmp_wdl_file )

    indata = {'Salmon.sample_name': name,
              "Salmon.fwd_reads": os.path.abspath(fwd_reads),
              "Salmon.threads": 6,
              "Salmon.reference_dir": reference}

    if rev_reads is not None:
        indata["Salmon.rev_reads"] = os.path.abspath(rev_reads)

    tmp_inputs = write_tmp_json( indata )
    tmp_options = outdir_json( outdir )
    tmp_labels = labels_json(workflow='salmon', env=env, sample=name)
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)

    if env == 'development':
        print(f"wdl: {tmp_wf_file}, inputs:{tmp_inputs}, options:{tmp_options}, labels:{tmp_labels}")

    st = cromwell_api.submit_workflow(wdl_file=tmp_wf_file, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    del_files( tmp_wf_file, tmp_inputs, tmp_options, tmp_labels)    

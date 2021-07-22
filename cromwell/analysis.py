import os
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



def write_tmp_json(data) -> str:

    tmpfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    json.dump(data, tmpfile.file)
    tmpfile.close()

    return tmpfile.name



def exomes_subcmd(analysis:str, args:list, outdir:str=None,unmapped_bam_suffix:str=".ubam") -> None:


    if 'help' in args or 'h' in args:
        print("Help:")
        print("Exomes/genomes analysis from unaligned bams (ubams)")
        print("==========================")
        print("exome <input-files> [-r reference] ")
        print("genome <input-files> [-r reference] [base-recalibration]")
        sys.exit(1)


    args_utils.min_count(1, len(args), msg="One or more ubams required.")

    for arg in args:
        sample_name = re.sub('\..*', '', arg)
#        print( sample_name, arg)
        exome_subcmd(analysis, [sample_name, arg], outdir=outdir)


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
              f"sample_and_unmapped_bams.base_filename={name}"]


    if analysis == 'genome':
        indata['WGS'] = True
        indata['doBSQR'] = True

    data = json_utils.build_json(indata, "DNAPreprocessing")

    data["DNAPreprocessing"]['sample_and_unmapped_bams']['unmapped_bams'] = []
    for infile in infiles:
        data["DNAPreprocessing"]['sample_and_unmapped_bams']['unmapped_bams'].append( infile )

    
    data = json_utils.add_jsons(data, [reference], "DNAPreprocessing")
    data = json_utils.pack(data, 2)
    tmp_inputs = write_tmp_json( data )
    
    tmp_options = None
    if outdir is not None:
        tmp_options={"final_workflow_outputs_dir": outdir, "use_relative_output_paths": True}
        tmp_options = write_tmp_json(tmp_options)


    tmp_labels = write_tmp_json({"env": env, "user": getpass.getuser()})
    print(f"wdl: {wdl_wf}, inputs:{tmp_inputs}, options:{tmp_options}, labels:{tmp_labels}")

    st = cromwell_api.submit_workflow(wdl_file=wdl_wf, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")


def utils_subcmd(args:list, outdir:str=None,unmapped_bam_suffix:str=".ubam") -> None:
    commands = {'bu':'bam-to-ubam', 'fu':'fq-to-ubam'}
    
    command = args.pop(0)
    if command in commands:
        command = commands[command]

    if 'help' in args or 'h' in args or command != 'bam-to-ubam':
        print("Help:")
        print("Pipelined utils")
        print("==========================")
        print("bam-to-ubam <bams>")
        sys.exit(1)

    data = {"BamToUnalignedBam.unmapped_bam_suffix":'.ubam',
             "BamToUnalignedBam.mapped_bam_suffix":'.bam',
             "BamToUnalignedBam.outdir":".",
             "BamToUnalignedBam.bams": []}

    for arg in args:
        data["BamToUnalignedBam.bams"].append( os.path.abspath( arg ))
 
    tmp_inputs = write_tmp_json( data )
    
    tmp_options = None
    if outdir is not None:
        tmp_options={"final_workflow_outputs_dir": outdir}


    tmp_labels = write_tmp_json({"env": env, "user": getpass.getuser()})
    print(f"wdl: {wf_files['bam-to-ubam']}, inputs:{tmp_inputs}, options:{tmp_options}, labels:{tmp_labels}")

    st = cromwell_api.submit_workflow(wdl_file=wf_files['bam-to-ubam'], inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=nsm_zip)
    print(f"{st['id']}: {st['status']}")




def callvars_subcmd(analysis:str, args:list, outdir:str=None,unmapped_bam_suffix:str=".ubam") -> None:

    if 'help' in args or 'h' in args:
        print("Help:")
        print("bam --> gvcf")
        print("==========================")
        print("call-variants <sample-name> <input-files> [-r reference] ")
        print("call-variants <sample-name> <input-files> [-r reference] [base-recalibration]")
        sys.exit(1)


    name    = args_utils.get_or_fail(args, "Sample name is required")
    bamfile = args_utils.get_or_fail(args, "bam file required.")
    bamfile = os.path.abspath(bamfile)
    bam_index = re.sub(r'\.bam\z', '.bai', bamfile)


    indata = [f'input_bam={bamfile}',
              f'input_bam_index={bam_index}',
              f"base_file_name={name}",
              f"final_vcf_base_name={name}",
            ]



    data = json_utils.build_json(indata, "VariantCalling")

    data["VariantCalling"]["scatter_settings"] = {"haplotype_scatter_count": 10,
                                                  "break_bands_at_multiples_of": 0 }

    data["VariantCalling"]["snp_recalibration_tranche_values"] = ["100.0", "99.95", "99.9", "99.8", "99.7", "99.6", "99.5", "99.4", "99.3", "99.0", "98.0", "97.0", "90.0"]
    data["VariantCalling"]["snp_recalibration_annotation_values"]=["AS_QD", "AS_MQRankSum", "AS_ReadPosRankSum", "AS_FS", "AS_MQ", "AS_SOR"]
    data["VariantCalling"]["indel_recalibration_tranche_values"]=["100.0", "99.95", "99.9", "99.5", "99.0", "97.0", "96.0", "95.0", "94.0", "93.5", "93.0", "92.0", "91.0", "90.0"]
    data["VariantCalling"]["indel_recalibration_annotation_values"]=["AS_FS", "AS_ReadPosRankSum", "AS_MQRankSum", "AS_QD", "AS_SOR"]
    data["VariantCalling"]["snp_filter_level"]=99.7
    data["VariantCalling"]["indel_filter_level"]=95.0

    data = json_utils.add_jsons(data, [reference], "VariantCalling")
    data = json_utils.pack(data, 2)
    tmp_inputs = write_tmp_json( data )
    
    tmp_options = None
    if outdir:
        tmp_options = write_tmp_json({"final_workflow_outputs_dir": outdir})

    tmp_labels = write_tmp_json({"env": env, "user": getpass.getuser()})
    print(f"wdl: {wf_files['mapping_dna']}, inputs:{tmp_inputs}, options:{tmp_options}, labels:{tmp_labels}")



    st = cromwell_api.submit_workflow(wdl_file=wf_files['var_calling'], inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency="/cluster/lib/nsm-analysis2/nsm-analysis.zip")
    print(f"{st['id']}: {st['status']}")


def salmon(args:str, reference:str, wdl_wf:str, wdl_zip:str=None, outdir:str=None, env:str=None ) -> None:

    name = args_utils.get_or_fail(args, "Sample name is missing")
    fwd_reads = args_utils.get_or_fail(args, "fwd-reads file missing")
    rev_reads = args_utils.get_or_default(args, None)


    indata = {'Salmon.sample_name': name,
              "Salmon.fwd_reads": fwd_reads",
              "Salmon.threads": 6,
              "Salmon.reference": reference}

    if rev_reads is not None:
        indata["Salmon.rev_reads"] = rev_reads

    tmp_inputs = write_tmp_json( indata )
    
    tmp_options = None
    if outdir is not None:
        tmp_options={"final_workflow_outputs_dir": outdir, "use_relative_output_paths": True}
        tmp_options = write_tmp_json(tmp_options)

    tmp_labels = write_tmp_json({"env": env, "user": getpass.getuser()})
    if env == 'development':
        print(f"wdl: {wdl_wf}, inputs:{tmp_inputs}, options:{tmp_options}, labels:{tmp_labels}")

    st = cromwell_api.submit_workflow(wdl_file=wdl_wf, inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency=wdl_zip)
    print(f"{st['id']}: {st['status']}")
    

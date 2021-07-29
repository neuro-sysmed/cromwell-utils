def callvars_subcmd(analysis:str, args:list, outdir:str=None,env:str=None) -> None:

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
    tmp_wf_file = cromwell_utils.fix_wdl_workflow_imports(wdl_wf)
    
    tmp_options = None
    if outdir:
        tmp_options = write_tmp_json({"final_workflow_outputs_dir": outdir})

    tmp_labels = write_tmp_json({"env": env, "user": getpass.getuser()})
    print(f"wdl: {tmp_wf_file}, inputs:{tmp_inputs}, options:{tmp_options}, labels:{tmp_labels}")

    st = cromwell_api.submit_workflow(wdl_file=wf_files['var_calling'], inputs=[tmp_inputs], options=tmp_options, labels=tmp_labels, dependency="/cluster/lib/nsm-analysis2/nsm-analysis.zip")
    print(f"{st['id']}: {st['status']}")


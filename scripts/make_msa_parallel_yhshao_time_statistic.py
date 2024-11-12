#!/usr/bin/env python3

import os
import argparse
import glob
import subprocess
from multiprocessing import Pool
import time
import csv
import re
import logging
from filelock import FileLock

# Set up logging
logging.basicConfig(filename='msa_stat/error_log.log', level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')

def handle_error(e):
    logging.error(f"Error occurred: {e}")

def process_fasta_wrapper(args):
    try:
        return process_fasta(*args)
    except Exception as e:
        error_message = f"Error in processing {args[0]}: {str(e)}"
        logging.error(error_message)
        raise RuntimeError(error_message)

def check_file_not_empty(file_path):
    """检查文件是否存在且不为空"""
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0

def check_output_files(out_dir):
    """检查所有预期的输出文件是否已存在且不为空"""
    expected_files = [
        os.path.join(out_dir, "t000_.msa0.a3m"),
        os.path.join(out_dir, "t000_.hhr"),
        os.path.join(out_dir, "t000_.atab")
    ]
    return all(check_file_not_empty(f) for f in expected_files)

def parse_time_output(output):
    print(output)

    # Extract relevant metrics using regular expressions
    sys_time = float(re.search(r"User time \(seconds\): (\d+\.\d+)", output).group(1)) / 60  # convert to minutes
    usr_time = float(re.search(r"System time \(seconds\): (\d+\.\d+)", output).group(1)) / 60  # convert to minutes

    # Extract clock time
    # First, try to match h:mm:ss format
    real_time = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): (\d+):(\d+):(\d+)", output)

    if real_time:  # h:mm:ss format
        print(f"h:mm:ss format, real_time: {real_time.group(1)} hours, {real_time.group(2)} minutes, {real_time.group(3)} seconds")
        real_time_minutes = int(real_time.group(1)) * 60 + int(real_time.group(2)) + int(real_time.group(3)) / 60
    else:
        # Try to match m:ss format
        real_time = re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): (\d+):(\d+\.\d+)", output)
        if real_time:  # m:ss format
            print(f"m:ss format, real_time: {real_time.group(1)} minutes, {real_time.group(2)} seconds")
            real_time_minutes = int(real_time.group(1)) + float(real_time.group(2)) / 60
        else:
            print("No match found for time format")
            real_time_minutes = None  # Handle the case where no match is found

    if real_time_minutes is not None:
        print(f"real_time_minutes: {real_time_minutes}")
    else:
        print("Clock time parsing failed")

    cpu_prop = float(re.search(r"Percent of CPU this job got: (\d+)%", output).group(1)) / 100  # proportion
    max_res = float(re.search(r"Maximum resident set size \(kbytes\): (\d+)", output).group(1)) / (1024**2)  # in GB
    io_read = float(re.search(r"File system inputs: (\d+)", output).group(1)) / (1024**2)  # in GB
    io_write = float(re.search(r"File system outputs: (\d+)", output).group(1)) / (1024**2)  # in GB

    return sys_time, usr_time, real_time_minutes, cpu_prop, max_res, io_read, io_write


def write_raw_output(output, csv_file_path, ID, length):
    # 使用FileLock确保多进程写入文件时不会发生冲突
    with FileLock(csv_file_path + ".lock"):
        # 将原始output写入CSV文件
        with open(csv_file_path, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # 如果文件为空，写入表头
            if os.stat(csv_file_path).st_size == 0:
                csvwriter.writerow(['ID', 'len', 'raw_output'])
            csvwriter.writerow([ID, length, output])

def parse_and_write_output(output, csv_file_path, ID, length):
    # 调用parse_time_output来解析output内容
    sys_time, usr_time, real_time, cpu_prop, max_res, io_read, io_write = parse_time_output(output)

    # 使用FileLock确保多进程写入文件时不会发生冲突
    with FileLock(csv_file_path + ".lock"):
        # 将解析结果写入CSV文件
        with open(csv_file_path, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # 如果文件为空，写入表头
            if os.stat(csv_file_path).st_size == 0:
                csvwriter.writerow(['ID', 'len', 'sys_time', 'usr_time', 'real_time', 'cpu_prop', 'max_res', 'io_read', 'io_write'])
            csvwriter.writerow([ID, length, round(sys_time, 2), round(usr_time, 2), round(real_time, 2), round(cpu_prop, 2), 
                                round(max_res, 2), round(io_read, 2), round(io_write, 2)])

def process_fasta(in_fasta, out_dir, db_templ, pipe_dir, ID, len):
    
    # 检查输出文件是否已存在且不为空
    if check_output_files(out_dir):
        print(f"所有输出文件已存在且不为空，跳过处理 {in_fasta}")
        return

    start = time.time()
    print(f"Processing {in_fasta} , output directory: {out_dir}")

    # Create output directory
    os.makedirs(out_dir, exist_ok=True)

    # Set resources
    cpu = "4"
    mem = "10000000"

    # Setup environment variables
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.environ['PIPE_DIR'] = script_dir

    # Sequence databases
    db_ur30 = os.path.join(script_dir, "UniRef30_2020_06/UniRef30_2020_06")
    db_bfd = os.path.join(script_dir, "bfd/bfd_metaclust_clu_complete_id30_c90_final_seq.sorted_opt")

    # Create a directory for statistics
    stat_dir = f"{pipe_dir}/msa_stat"
    os.makedirs(stat_dir, exist_ok=True)

    # <<< Run signalp6 with resource monitoring >>> 
    tmp_dir = os.path.join(out_dir, "signalp")
    os.makedirs(tmp_dir, exist_ok=True)

    cmd = f"""
    /usr/bin/time -v bash -c "
    signalp6 --fastafile {in_fasta} --organism other --output_dir {tmp_dir} --format none --mode slow
    "
    """
    # cmd = f"""
    # /usr/bin/time -v bash -c "
    # echo 'Running signalP 6.0, instruction: signalp6 --fastafile {in_fasta} --organism other --output_dir {tmp_dir} --format none --mode slow'
    # "
    # """

    # Run the command and capture the output
    result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
    output = result.stderr

    # Save statistics to CSV
    stat_file = f"{stat_dir}/signalp6_stat.csv"
    stat_file_raw = f"{stat_dir}/signalp6_stat_raw.csv"
    parse_and_write_output(output, stat_file, ID, len)
    write_raw_output(output, stat_file_raw, ID, len)

    trim_fasta= os.path.join(tmp_dir, "processed_entries.fasta")
    if not os.path.exists(trim_fasta) or os.path.getsize(trim_fasta) == 0:
        trim_fasta = in_fasta

    # <<< Run HHblits with resource monitoring >>>
    out_prefix = os.path.join(out_dir, "t000_")
    tmp_dir = os.path.join(out_dir, "hhblits")
    os.makedirs(tmp_dir, exist_ok=True)

    HHBLITS_UR30 = f"hhblits -o /dev/null -mact 0.35 -maxfilt 100000000 -neffmax 20 -cov 25 -cpu {cpu} -nodiff -realign_max 100000000 -maxseq 1000000 -maxmem {mem} -n 4 -d {db_ur30}"
    HHBLITS_BFD = f"hhblits -o /dev/null -mact 0.35 -maxfilt 100000000 -neffmax 20 -cov 25 -cpu {cpu} -nodiff -realign_max 100000000 -maxseq 1000000 -maxmem {mem} -n 4 -d {db_bfd}"

    prev_a3m = trim_fasta
    e_values = [1e-10, 1e-6, 1e-3]

    if not os.path.exists(f"{out_prefix}.msa0.a3m"):
        for e in e_values:
            print(f"Running HHblits against UniRef30 with E-value cutoff {e}")
            a3m_file = os.path.join(tmp_dir, f"t000_.{e}.a3m")
            if not os.path.exists(a3m_file):
                cmd = f"""
                /usr/bin/time -v bash -c "
                {HHBLITS_UR30} -i {prev_a3m} -oa3m {a3m_file} -e {e} -v 0
                "
                """
                # cmd = f"""
                # /usr/bin/time -v bash -c "
                # echo 'Running HHblits against UniRef30 with E-value cutoff {e}, instruction: {HHBLITS_UR30} -i {prev_a3m} -oa3m {a3m_file} -e {e} -v 0'
                # cp {out_dir}/materials/t000_.{e}.a3m {a3m_file}
                # "
                # """
                    
                # Run the command and capture the output
                result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
                output = result.stderr

                # Save statistics to CSV
                stat_file = f"{stat_dir}/hhblits_stat_uniref_{e}.csv"
                stat_file_raw = f"{stat_dir}/hhblits_stat_uniref_raw_{e}.csv"
                parse_and_write_output(output, stat_file, ID, len)
                write_raw_output(output, stat_file_raw, ID, len)

            # <<< Run hhfilter 75 with resource monitoring >>>
            print(f"Running hhfilter 75 with E-value cutoff {e}")
            cmd = f"""
            /usr/bin/time -v bash -c "
            hhfilter -maxseq 100000 -id 90 -cov 75 -i {a3m_file} -o {tmp_dir}/t000_.{e}.id90cov75.a3m
            "
            """

            # Run the command and capture the output
            result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
            output = result.stderr

            # Save statistics to CSV
            stat_file = f"{stat_dir}/hhfilter_stat_75_uniref_{e}.csv"
            stat_file_raw = f"{stat_dir}/hhfilter_stat_75_uniref_raw_{e}.csv"
            parse_and_write_output(output, stat_file, ID, len)
            write_raw_output(output, stat_file_raw, ID, len)
            
            n75 = int(subprocess.getoutput(f"grep -c '^>' {tmp_dir}/t000_.{e}.id90cov75.a3m"))
            # move the filtered a3m file to the output directory if the number of sequences is greater than the threshold
            if n75 > 2000 and not os.path.exists(f"{out_prefix}.msa0.a3m"):
                os.rename(f"{tmp_dir}/t000_.{e}.id90cov75.a3m", f"{out_prefix}.msa0.a3m")
                print(f"Found {n75} sequences in {tmp_dir}/t000_.{e}.id90cov75.a3m, breaking the loop.")
                break

            # <<< Run hhfilter 50 with resource monitoring >>>
            print(f"Running hhfilter 50 with E-value cutoff {e}")
            cmd = f"""
            /usr/bin/time -v bash -c "
            hhfilter -maxseq 100000 -id 90 -cov 50 -i {a3m_file} -o {tmp_dir}/t000_.{e}.id90cov50.a3m
            "
            """

            # Run the command and capture the output
            result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
            output = result.stderr

            # Save statistics to CSV
            stat_file = f"{stat_dir}/hhfilter_stat_50_uniref_{e}.csv"
            stat_file_raw = f"{stat_dir}/hhfilter_stat_50_uniref_raw_{e}.csv"
            parse_and_write_output(output, stat_file, ID, len)
            write_raw_output(output, stat_file_raw, ID, len)

            n50 = int(subprocess.getoutput(f"grep -c '^>' {tmp_dir}/t000_.{e}.id90cov50.a3m"))
            # move the filtered a3m file to the output directory if the number of sequences is greater than the threshold
            if n50 > 4000 and not os.path.exists(f"{out_prefix}.msa0.a3m"):
                os.rename(f"{tmp_dir}/t000_.{e}.id90cov50.a3m", f"{out_prefix}.msa0.a3m")
                print(f"Found {n50} sequences in {tmp_dir}/t000_.{e}.id90cov50.a3m, breaking the loop.")
                break
            
            # Set the next iteration's input file
            prev_a3m = f"{tmp_dir}/t000_.{e}.id90cov50.a3m"
            print(f"Found {n50} sequences in {tmp_dir}/t000_.{e}.id90cov50.a3m, continuing to the next iteration.")


        if not os.path.exists(f"{out_prefix}.msa0.a3m"):
            print("Running HHblits against BFD")
            e = 1e-3
            bfd_file = os.path.join(tmp_dir, f"t000_.{e}.bfd.a3m")
            if not os.path.exists(bfd_file):
                cmd = f"""
                /usr/bin/time -v bash -c "
                {HHBLITS_BFD} -i {prev_a3m} -oa3m {bfd_file} -e {e} -v 0
                "
                """
                # cmd = f"""
                # /usr/bin/time -v bash -c "
                # echo 'Running HHblits against BFD with E-value cutoff {e}, instruction: {HHBLITS_BFD} -i {prev_a3m} -oa3m {bfd_file} -e {e} -v 0'
                # cp {out_dir}/materials/t000_.{e}.bfd.a3m {bfd_file}
                # "
                # """

                # Run the command and capture the output
                result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
                output = result.stderr

                # Save statistics to CSV
                stat_file = f"{stat_dir}/hhblits_stat_bfd.csv"
                stat_file_raw = f"{stat_dir}/hhblits_stat_bfd_raw.csv"
                parse_and_write_output(output, stat_file, ID, len)
                write_raw_output(output, stat_file_raw, ID, len)

            # <<< Run hhfilter with resource monitoring >>>
            print(f"Running hhfilter 75 and 50 with E-value cutoff {e}")
            cmd = f"""
            /usr/bin/time -v bash -c "
            hhfilter -maxseq 100000 -id 90 -cov 75 -i {bfd_file} -o {tmp_dir}/t000_.{e}.bfd.id90cov75.a3m
            hhfilter -maxseq 100000 -id 90 -cov 50 -i {bfd_file} -o {tmp_dir}/t000_.{e}.bfd.id90cov50.a3m
            "
            """

            # Run the command and capture the output
            result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
            output = result.stderr

            # Save statistics to CSV
            stat_file = f"{stat_dir}/hhfilter_stat_bfd.csv"
            stat_file_raw = f"{stat_dir}/hhfilter_stat_raw_bfd.csv"
            parse_and_write_output(output, stat_file, ID, len)
            write_raw_output(output, stat_file_raw, ID, len)

            n75 = int(subprocess.getoutput(f"grep -c '^>' {tmp_dir}/t000_.{e}.bfd.id90cov75.a3m"))
            n50 = int(subprocess.getoutput(f"grep -c '^>' {tmp_dir}/t000_.{e}.bfd.id90cov50.a3m"))

            if n75 > 2000 and not os.path.exists(f"{out_prefix}.msa0.a3m"):
                os.rename(f"{tmp_dir}/t000_.{e}.bfd.id90cov75.a3m", f"{out_prefix}.msa0.a3m")
                print(f"Found {n75} sequences in {tmp_dir}/t000_.{e}.bfd.id90cov75.a3m, breaking the loop.")
            elif n50 > 4000 and not os.path.exists(f"{out_prefix}.msa0.a3m"):
                os.rename(f"{tmp_dir}/t000_.{e}.bfd.id90cov50.a3m", f"{out_prefix}.msa0.a3m")
                print(f"Found {n50} sequences in {tmp_dir}/t000_.{e}.bfd.id90cov50.a3m, breaking the loop.")

        if not os.path.exists(f"{out_prefix}.msa0.a3m"):
            os.rename(f"{tmp_dir}/t000_.{e}.bfd.id90cov50.a3m", f"{out_prefix}.msa0.a3m")
            print(f"Found {n50} sequences in {tmp_dir}/t000_.{e}.bfd.id90cov50.a3m, using it as the final MSA.")

    # <<< Run PSIPRED with resource monitoring >>>
    print("Running PSIPRED")
    tmp_dir = os.path.join(out_dir, "log")
    os.makedirs(tmp_dir, exist_ok=True)

    cmd = f"""
    /usr/bin/time -v bash -c "
    {pipe_dir}/input_prep/make_ss.sh {out_prefix}.msa0.a3m {out_prefix}.ss2 > {tmp_dir}/make_ss.stdout 2> {tmp_dir}/make_ss.stderr
    "
    """

    # Run the command and capture the output
    result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
    output = result.stderr

    # Save statistics to CSV
    stat_file = f"{stat_dir}/psipred_stat.csv"
    stat_file_raw = f"{stat_dir}/psipred_stat_raw.csv"
    parse_and_write_output(output, stat_file, ID, len)
    write_raw_output(output, stat_file_raw, ID, len)


    # <<< Run HHsearch with resource monitoring >>>
    if os.path.exists(f"{out_prefix}.msa0.a3m") and not os.path.exists(f"{out_prefix}.hhr"):
        print("Running hhsearch")
        HH = f"hhsearch -b 50 -B 500 -z 50 -Z 500 -mact 0.05 -cpu {cpu} -maxmem {mem} -aliw 100000 -e 100 -p 5.0 -d {db_templ}"
        cmd = f"""
        /usr/bin/time -v bash -c "
        cat {out_prefix}.ss2 {out_prefix}.msa0.a3m > {out_prefix}.msa0.ss2.a3m
        {HH} -i {out_prefix}.msa0.ss2.a3m -o {out_prefix}.hhr -atab {out_prefix}.atab -v 0
        "
        """

        # Run the command and capture the output
        result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, universal_newlines=True)
        output = result.stderr

        # Save statistics to CSV
        stat_file = f"{stat_dir}/hhsearch_stat.csv"
        stat_file_raw = f"{stat_dir}/hhsearch_stat_raw.csv"
        parse_and_write_output(output, stat_file, ID, len)
        write_raw_output(output, stat_file_raw, ID, len)

    end = time.time()
    print(f"#### finished processing {in_fasta}, processing time: {end - start} seconds. ####")

def main():
    parser = argparse.ArgumentParser(description="Process multiple FASTA files in parallel")
    parser.add_argument('-i', '--input_dir', required=True, help="Directory containing FASTA files")
    parser.add_argument('-o', '--output_dir', required=True, help="Base directory for output")
    parser.add_argument('-d', '--db_templ', required=True, help="Path to template database")
    parser.add_argument('-n', '--num_tasks', type=int, default=1, help="Number of parallel tasks to run")
    args = parser.parse_args()

    # Get all FASTA files in the input directory
    fasta_files = glob.glob(os.path.join(args.input_dir, "*.fasta"))

    # Prepare arguments for parallel processing
    tasks = []
    for fasta_file in fasta_files:
        base_name = os.path.basename(fasta_file).replace('.fasta', '') # OsMH_01T0631100.1_212.fasta -> OsMH_01T0631100.1_212
        out_dir = os.path.join(args.output_dir, f"{base_name}/A")

        # Get ID and len from fasta file
        ID = base_name.rsplit('_', 1)[0] # OsMH_01T0631100.1_212 -> OsMH_01T0631100.1
        length = base_name.rsplit('_', 1)[1] # OsMH_01T0631100.1_212 -> 212

        tasks.append((fasta_file, out_dir, args.db_templ, os.path.dirname(os.path.realpath(__file__)), ID, length))

    # Create a pool of workers and handle errors
    with Pool(processes=args.num_tasks) as pool:
        results = [pool.apply_async(process_fasta_wrapper, (task,), error_callback=handle_error) for task in tasks]
        
        # Close the pool and wait for tasks to complete
        pool.close()
        pool.join()

    # Collect results to ensure all jobs completed successfully
    for r in results:
        r.get()  # This will raise an exception if the task failed


    # for fasta_file in fasta_files:
    #     start = time.time()
    #     base_name = os.path.basename(fasta_file).replace('.fasta', '') # OsMH_01T0631100.1_212.fasta -> OsMH_01T0631100.1_212
    #     out_dir = os.path.join(args.output_dir, f"{base_name}/A")

    #     # Get ID and len from fasta file
    #     ID = base_name.rsplit('_', 1)[0]
    #     len = base_name.rsplit('_', 1)[1]

    #     file_size = os.path.getsize(fasta_file)
    #     print(f"/nProcessing {fasta_file} , file size: {file_size} bytes, output directory: {out_dir}")

    #     process_fasta(fasta_file, out_dir, args.db_templ, os.path.dirname(os.path.realpath(__file__)), ID, len)

    #     end = time.time()
    #     print(f"Processing time: {end - start} seconds./n")

if __name__ == "__main__":
    main()


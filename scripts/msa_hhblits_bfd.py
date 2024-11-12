import os
import subprocess

def terminate_hhblits_bfd():
    # TODO: change job node information, switch to next queue, clear temp files
    return

# e_values = 1e-3
def run_hhblits_bfd(out_dir, in_fasta, cpu, mem, db_bfd, e_value, log_file):
    final_msa = os.path.join(out_dir, "t000_.msa0.a3m")
    tmp_dir = os.path.join(out_dir, "hhblits")
    os.makedirs(tmp_dir, exist_ok=True)

    HHBLITS_BFD = f"hhblits -o /dev/null -mact 0.35 -maxfilt 100000000 -neffmax 20 -cov 25 -cpu {cpu} -nodiff -realign_max 100000000 -maxseq 1000000 -maxmem {mem} -n 4 -d {db_bfd}"

    if not os.path.exists(final_msa):
        # <<< Run HHblits against BFD >>>
        print(f"Running HHblits against BFD with E-value cutoff {e_value}")
        a3m_file = os.path.join(tmp_dir, f"t000_.{e_value}.bfd.a3m")
        if not os.path.exists(a3m_file):
            cmd = f"""
            {HHBLITS_BFD} -i {in_fasta} -oa3m {a3m_file} -e {e_value} -v 0
            """
            print(cmd)
            subprocess.run(cmd, shell=True, check=True)
        else:
            print(f"Found {a3m_file}, skipping HHblits against BFD with E-value cutoff {e_value}.")


        # <<< Run hhfilter with 90% identity and 75% coverage >>>
        a3m_file_id90cov75 = os.path.join(tmp_dir, f"t000_.{e_value}.id90cov75.bfd.a3m")
        if not os.path.exists(a3m_file_id90cov75):
            print(f"Running hhfilter on {final_msa} with E-value cutoff {e_value}, 90% identity, and 75% coverage")
            cmd = f"""
            hhfilter -maxseq 100000 -id 90 -cov 75 -i {a3m_file} -o {a3m_file_id90cov75}
            """
            print(cmd)
            subprocess.run(cmd, shell=True, check=True)
        else:
            print(f"Found {a3m_file_id90cov75}, skipping HHfilter with 90% identity and 75% coverage.")


        # <<< Run hhfilter with 90% identity and 50% coverage >>>
        a3m_file_id90cov50 = os.path.join(tmp_dir, f"t000_.{e_value}.id90cov50.bfd.a3m")
        if not os.path.exists(a3m_file_id90cov50):
            print(f"Running hhfilter on {final_msa} with E-value cutoff {e_value}, 90% identity, and 50% coverage")
            cmd = f"""
            hhfilter -maxseq 100000 -id 90 -cov 50 -i {a3m_file} -o {a3m_file_id90cov50}
            """
            print(cmd)
            subprocess.run(cmd, shell=True, check=True)
        else:
            print(f"Found {a3m_file_id90cov50}, skipping HHfilter with 90% identity and 50% coverage.")
    

        # <<< Check the number of sequences in the filtered a3m file >>>
        n75 = int(subprocess.getoutput(f"grep -c '^>' {a3m_file_id90cov75}"))
        n50 = int(subprocess.getoutput(f"grep -c '^>' {a3m_file_id90cov50}"))
        if n75 > 2000 and not os.path.exists(final_msa):
            os.system(f"cp {a3m_file_id90cov75} {final_msa}")
            print(f"Found {n75} sequences in {a3m_file_id90cov75}, finishing the HHblits process.")
            return
        elif n50 > 4000 and not os.path.exists(final_msa):
            os.system(f"cp {a3m_file_id90cov50} {final_msa}")
            print(f"Found {n50} sequences in {a3m_file_id90cov50}, finishing the HHblits process.")
            return
        
        os.system(f"cp {a3m_file_id90cov50} {final_msa}")
        print(f"Failed to get enough sequences from BFD, using {a3m_file_id90cov50} with {n50} sequences as the final MSA.")
        return

    else:
        print(f"Found final result file: {final_msa}, skipping HHblits process.")
        return
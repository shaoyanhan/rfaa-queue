import os
import subprocess

def run_signalp6(out_dir, in_fasta, log_file):
    tmp_dir = os.path.join(out_dir, "signalp")
    os.makedirs(tmp_dir, exist_ok=True)

    cmd = f"""
    signalp6 --fastafile {in_fasta} --organism other --output_dir {tmp_dir} --format none --mode slow
    """    
    print(cmd)
    subprocess.run(cmd, shell=True, check=True)
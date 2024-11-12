import os
import subprocess

def terminate_psipred():
    # TODO: change job node information, switch to next queue, clear temp files
    return

def run_psipred(out_dir, pipe_dir, log_file):
    print("Running PSIPRED")
    out_prefix = os.path.join(out_dir, "t000_")
    final_msa = f"{out_prefix}.msa0.a3m"
    tmp_dir = os.path.join(out_dir, "log")
    os.makedirs(tmp_dir, exist_ok=True)

    if os.path.exists(final_msa):
        cmd = f"""
        {pipe_dir}/input_prep/make_ss.sh {final_msa} {out_prefix}.ss2 > {tmp_dir}/make_ss.stdout 2> {tmp_dir}/make_ss.stderr
        """
        print(cmd)
        subprocess.run(cmd, shell=True, check=True)
    else:
        print(f"Missing {final_msa}, stopping PSIPRED.")
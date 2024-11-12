import os
import subprocess

def terminate_hhsearch():
    # TODO: change job node information, switch to next queue, clear temp files
    return

def run_hhsearch(out_dir, cpu, mem, db_pdb70, log_file):
    out_prefix = os.path.join(out_dir, "t000_")
    final_msa = f"{out_prefix}.msa0.a3m"
    
    if os.path.exists(final_msa):
        if os.path.exists(f"{out_prefix}.hhr") and os.path.exists(f"{out_prefix}.atab"):
            print(f"Found {out_prefix}.hhr and {out_prefix}.atab, skipping HHsearch.")
            return
        
        print("Running hhsearch")
        HH = f"hhsearch -b 50 -B 500 -z 50 -Z 500 -mact 0.05 -cpu {cpu} -maxmem {mem} -aliw 100000 -e 100 -p 5.0 -d {db_pdb70}"
        cmd = f"""
        cat {out_prefix}.ss2 {out_prefix}.msa0.a3m > {out_prefix}.msa0.ss2.a3m
        {HH} -i {out_prefix}.msa0.ss2.a3m -o {out_prefix}.hhr -atab {out_prefix}.atab -v 0
        """
        print(cmd)
        subprocess.run(cmd, shell=True, check=True)
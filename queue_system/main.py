import argparse
import yaml
import sys
import os
import json

# Load configuration from YAML file
def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Merge user params with defaults, overriding defaults if user provided values
def merge_params(default_params, user_params):
    for key, value in user_params.items():
        if isinstance(value, dict) and key in default_params:
            default_params[key] = merge_params(default_params[key], value)
        else:
            default_params[key] = value
    return default_params

# Validate required parameters
def validate_params(params):
    required_keys = ["input_config_path", "output_path", "job_core_num", "job_mem_num"]
    missing_keys = [key for key in required_keys if key not in params]
    if missing_keys:
        print(f"Error: Missing required parameter(s): {', '.join(missing_keys)}")
        print_help()
        sys.exit(1)

# Parse JSON input for complex parameters
def parse_json_param(param_str):
    try:
        return json.loads(param_str)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format for parameter: {param_str}")
        print_help()
        sys.exit(1)

# Help message
def print_help():
    help_message = """
Usage: 
  1. python queue_system/main.py [OPTIONS]
  2. python -m queue_system.main [OPTIONS]

Options:
  -i, --input_config_path PATH     Path to the input configuration files.
  -o, --output_path PATH           Path to the output directory.
  -p, --job_core_num JSON          JSON string for core numbers per job type.
  -m, --job_mem_num JSON           JSON string for memory allocation per job type.
  -k, --total_core_num INT         Total number of cores (default: auto).
  -e, --total_mem_num INT          Total memory in GB (default: auto).
  -b, --mem_buffer INT             Memory buffer in GB (default: 10).
  -a, --wait_time_max FLOAT        Maximum wait time percentage (default: 10).
  -d, --wait_time_mid FLOAT        Mid-level wait time percentage (default: 5).
  -f, --config FILE                Path to the configuration.yaml file.
  -h, --help                       Show this help message and exit.
    """
    print(help_message)

def main():
    # Default config path
    default_config_path = "configuration.yaml"
    
    # Argument parsing with short and long options
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-i", "--input_config_path", type=str, help="Path to the input configuration files")
    parser.add_argument("-o", "--output_path", type=str, help="Path to the output directory")
    parser.add_argument("-p", "--job_core_num", type=str, help="Core numbers per job type in JSON format")
    parser.add_argument("-m", "--job_mem_num", type=str, help="Memory allocation per job type in JSON format")
    parser.add_argument("-k", "--total_core_num", type=str, help="Total number of cores (default: auto)")
    parser.add_argument("-e", "--total_mem_num", type=str, help="Total memory in GB (default: auto)")
    parser.add_argument("-b", "--mem_buffer", type=int, help="Memory buffer in GB (default: 10)")
    parser.add_argument("-a", "--wait_time_max", type=float, help="Maximum wait time percentage (default: 10)")
    parser.add_argument("-d", "--wait_time_mid", type=float, help="Mid-level wait time percentage (default: 5)")
    parser.add_argument("-f", "--config", type=str, help="Path to the configuration.yaml file")
    parser.add_argument("-h", "--help", action="store_true", help="Show this help message and exit")

    args = parser.parse_args()
    
    # If --help or -h is specified, print help and exit
    if args.help:
        print_help()
        sys.exit(0)
    
    # Load default configuration from YAML
    config_path = args.config if args.config else default_config_path
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.")
        print_help()
        sys.exit(1)
        
    default_params = load_config(config_path)
    
    # Override default configuration with command-line arguments
    user_params = vars(args)
    config_params = {k: v for k, v in user_params.items() if v is not None}

    # Parse JSON strings for complex parameters
    if 'job_core_num' in config_params:
        config_params['job_core_num'] = parse_json_param(config_params['job_core_num'])
    if 'job_mem_num' in config_params:
        config_params['job_mem_num'] = parse_json_param(config_params['job_mem_num'])
    
    # Merge the user-provided parameters with the default configuration
    final_params = merge_params(default_params, config_params)
    
    # Validate final configuration
    validate_params(final_params)
    
    # Display the final configuration (for debugging, remove in production)
    print("Final Configuration:")
    for key, value in final_params.items():
        print(f"{key}: {value}")

    # Main functionality goes here
    # e.g., initialize processing based on final_params
    
if __name__ == "__main__":
    main()
"""

@author Jason Yi
@date May 12 2024

"""
import subprocess
import argparse
import modal
from modal import App, Image

protein_app = App(image=Image.debian_slim().apt_install("python3-pip").pip_install(["biopython"]))

def execute_prediction(process_data):
    prediction_results = {}
    for identifier, sequence in process_data:
        print(f"Processing sequence {identifier}")
        process_output = subprocess.check_output(["python", "run_model.py", sequence], shell=False).decode('utf-8')
        prediction_results[identifier] = process_output
    return prediction_results

def main(input_file_path, result_file_path):
    sequence_data = load_sequences(input_file_path)
    sequence_batches = distribute_batches(sequence_data, 10)  
    manage_batches(sequence_batches, result_file_path)

def load_sequences(file_path):
    loaded_data = []
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                loaded_data.append(parts)
    return loaded_data

def distribute_batches(data, workers_count):
    batch_length = len(data) // workers_count
    return [data[i:i + batch_length] for i in range(0, len(data), batch_length)]

def manage_batches(batches, output_file):
    future_results = [execute_prediction.remote(batch) for batch in batches]
    for result in modal.wait(future_results):
        compile_results(output_file, result.result())

def compile_results(file_name, results):
    with open(file_name, "a") as output_file:
        for seq_id, predicted_structure in results.items():
            output_file.write(f"{seq_id}\t{predicted_structure}\n")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Process protein sequences to predict their secondary structures.')
    parser.add_argument('input_file', type=str, help='File path for input sequences.')
    parser.add_argument('output_file', type=str, help='File path for output.')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.output_file)




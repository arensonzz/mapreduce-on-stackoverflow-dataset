import subprocess
import time
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter


def run_hadoop_job(input_path, output_path):
    # Run the Hadoop job
    start_time = time.time()
    subprocess.call(['./hadoop', 'jar', '/app/jars/mapreduce-stackoverflow-1.0.jar',
                     'AnswerScoreStatistics', input_path, output_path])
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time


def run_python_job(input_path, output_path):
    answers = pd.read_csv(input_path, engine='c',
                          dtype={'Id': 'Int64', 'OwnerUserId': 'Int64',
                                 'CreationDate': str,
                                 'ParentId': 'Int64', 'Score': 'Int64',
                                 'Body': str})

    print(answers.head())

    # Run the Python word count job
    start_time = time.time()

    # Perform the word count task in Python
    data = pd.read_csv(input_path)
    upvote_scores = data.iloc[:, 4]  # 5th column name

    # Calculate statistics
    count = len(upvote_scores)
    average = upvote_scores.mean()
    median = upvote_scores.median()
    minimum = upvote_scores.min()
    maximum = upvote_scores.max()
    standard_deviation = upvote_scores.std()

    # Save the statistics to the output file
    with open(output_path, 'w') as file:
        file.write(f"Count: {count}\n")
        file.write(f"Average: {average}\n")
        file.write(f"Median: {median}\n")
        file.write(f"Minimum: {minimum}\n")
        file.write(f"Maximum: {maximum}\n")
        file.write(f"Standard Deviation: {standard_deviation}\n")

    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time


def generate_performance_figures(input_sizes, hadoop_times, python_times):
    # Generate performance figures
    plt.figure(figsize=(8, 6))
    plt.plot(input_sizes, hadoop_times, marker='o', label='Hadoop')
    plt.plot(input_sizes, python_times, marker='o', label='Python')
    plt.xlabel('Input Size')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Hadoop vs Python Job Performance')
    plt.grid(True)
    plt.legend()
    plt.savefig('performance_figure.png')
    plt.show()


def main():
    input_sizes = [100, 1000, 10000, 100000, 500000, 1000000, 1900000]  # Sample input sizes
    #  input_sizes = [100]  # Sample input sizes
    hadoop_times = []
    python_times = []
    input_path = './jobs/data/AnswersPre.csv'  # Path to the input file
    input_data = pd.read_csv(input_path)

    for size in input_sizes:

        # Generate the input file for the desired size
        data = input_data.iloc[:size, :]

        # Save the input data to a temporary file
        input_csv = 'temp_input.csv'
        data.to_csv(f'./jobs/data/{input_csv}', index=False, header=False)
        subprocess.call(['./hdfs', 'dfs', '-rm', '-r', '-f', '/output/evaluation*'])
        subprocess.call(['./hdfs', 'dfs', '-rm', '-r', '-f', f'/input/{input_csv}'])
        subprocess.call(['./hdfs', 'dfs', '-put', f'/app/data/{input_csv}', '/input'])

        hadoop_output_path = f'/output/evaluation_{size}_hadoop'

        python_output_path = f'./jobs/res/evaluation_{size}_python'
        hadoop_time = run_hadoop_job(f'/input/{input_csv}', hadoop_output_path)

        subprocess.call(['./hdfs', 'dfs', '-get', '/output/evaluation*', '/app/res'])

        #  ./hdfs dfs -get /output/${@} /app/res
        python_time = run_python_job(f'./jobs/data/{input_csv}', python_output_path)

        hadoop_times.append(hadoop_time)
        python_times.append(python_time)
    # Remove the temporary input file
    subprocess.call(['rm', f'./jobs/data/{input_csv}'])

    generate_performance_figures(input_sizes, hadoop_times, python_times)


if __name__ == '__main__':
    main()

import re

log_file = open("server-1.run.log", "r")

marker = "[Perf]"

latencies = {}
units = {}

for line in log_file:
    if line.startswith(marker):
        line = line[len(marker):]
        elements = re.split(': | ', line)
        tag, value, unit = elements[0], elements[1], elements[2]

        if tag in latencies:
            latencies[tag].append(float(value))
        else:
            latencies[tag] = [float(value)]
            units[tag] = unit

header = "Phase,Avg,Median,Min,Max,Stddev,Unit"

with open("latencies.csv", "w") as latency_csv:
    latency_csv.write(header + "\n")
    for tag in latencies:
        avg = sum(latencies[tag]) / len(latencies[tag])
        median = sorted(latencies[tag])[len(latencies[tag]) // 2]
        min_value = min(latencies[tag])
        max_value = max(latencies[tag])
        stddev = (sum([(x - avg) ** 2 for x in latencies[tag]]) / len(latencies[tag])) ** 0.5
        latency_csv.write(f"{tag},{avg},{median},{min_value},{max_value},{stddev},{units[tag]}")
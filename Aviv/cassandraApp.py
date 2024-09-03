import sys
import importlib
import threading
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
import json
import random
import datetime
import re
import matplotlib.pyplot as plt
from queue import Queue

def check_required_packages():
    required_packages = ['cassandra', 'matplotlib']
    missing_packages = []

    for package in required_packages:
        try:
            importlib.import_module(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print("The following required packages are missing:")
        for package in missing_packages:
            print(f"- {package}")
        print("\nPlease install these packages using:")
        print(f"pip install {' '.join(missing_packages)}")
        sys.exit(1)

# Check for required packages
check_required_packages()

class CassandraWorkload:
    def __init__(self):
        # Cassandra connection
        self.auth_provider = PlainTextAuthProvider(username='omrino', password='sfgs44Df')
        self.cluster = Cluster(
            contact_points=['62.90.89.27', '62.90.89.28', '62.90.89.29', '62.90.89.39'],
            auth_provider=self.auth_provider
        )
        self.session = self.cluster.connect('simpletry')
        self.session.default_consistency_level = ConsistencyLevel.ONE

        # Initialize results storage
        self.traces_res = []
        self.queries_and_times = []
        self.lock = threading.Lock()

    def setup_table(self):
        self.session.execute("DROP TABLE IF EXISTS simpletry.person;")
        self.session.execute("CREATE TABLE IF NOT EXISTS simpletry.person (id text, name text, toTS MAP<text,timestamp>, PRIMARY KEY (id));")

    def generate_workload(self, num_inserts, num_updates, use_timestamp):
        workload_commands = []
        person_id = 208306068
        current_number = 0
        timestamp_counter = 1

        for i in range(1, num_inserts + 1):
            person_name = f'omri{i}'
            time_stamp_id = f'{i}'
            if use_timestamp:
                command = f"INSERT INTO simpletry.person (id, name, toTS) VALUES ('{person_id}', '{person_name}', {{'{time_stamp_id}':toTimestamp(now())}}) USING TIMESTAMP {timestamp_counter}"
            else:
                command = f"INSERT INTO simpletry.person (id, name, toTS) VALUES ('{person_id}', '{person_name}', {{'{time_stamp_id}':toTimestamp(now())}})"
            workload_commands.append(command)
            current_number = i
            timestamp_counter += 1

        for i in range(1, num_updates + 1):
            person_name = f'Aviv/Gil{current_number + i}'
            time_stamp_id = f'{current_number + i}'
            command = f"UPDATE simpletry.person SET name = '{person_name}', toTS['{time_stamp_id}'] = toTimestamp(now()) WHERE id = '{person_id}'"
            workload_commands.append(command)
            timestamp_counter += 1

        return workload_commands

    def save_workload_to_file(self, filename, workload_commands):
        with open(filename, 'w') as file:
            for command in workload_commands:
                file.write(command + '\n')

    def execute_command(self, command):
        consistency_level = ConsistencyLevel.ONE
        query = SimpleStatement(command, consistency_level=consistency_level)

        try:
            res = self.session.execute(query, trace=True)

            trace = None
            retry_count = 0
            while not trace and retry_count < 3:
                try:
                    trace = res.get_query_trace(max_wait_sec=10)
                except Exception as e:
                    print(f"Attempt {retry_count + 1} to get trace failed: {str(e)}")
                    retry_count += 1
                    if retry_count == 3:
                        print(f"Failed to get trace after 3 attempts for query: {command}")

            execution_time = datetime.datetime.now().isoformat()

            if trace:
                coordinator_timestamp = None
                memtable_timestamp = None
                events_list = []

                for event in trace.events:
                    events = {
                        "description": event.description,
                        "source": event.source,
                        "source_elapsed": event.source_elapsed,
                        "thread_name": event.thread_name,
                        "datetime": event.datetime.isoformat(),
                    }
                    events_list.append(events)

                    if event.description in ("Enqueuing response to /", "Adding to memtable", "Adding to person memtable"):
                        memtable_timestamp = event.datetime.isoformat()
                    if event.description in ("Determining replicas for mutation", "Parsing", "Preparing statement"):
                        coordinator_timestamp = event.datetime.isoformat()

                data = {
                    "trace_id": trace.trace_id,
                    "request_type": trace.request_type,
                    "client": trace.client,
                    "coordinator": trace.coordinator,
                    "started_at": trace.started_at.isoformat(),
                    "parameters": trace.parameters,
                    "duration": str(trace.duration),
                    "query": command,
                    "events": events_list,
                    "coordinator_timestamp": coordinator_timestamp or execution_time,
                    "memtable_timestamp": memtable_timestamp,
                }

                with self.lock:
                    self.traces_res.append(data)
            else:
                print(f"No trace information available for query: {command}")

            with self.lock:
                self.queries_and_times.append([command, execution_time])

        except Exception as e:
            print(f"Error executing query: {command}")
            print(f"Error details: {str(e)}")

    def worker(self, command_queue):
        while not command_queue.empty():
            command = command_queue.get()
            if command is None:
                break
            self.execute_command(command)
            command_queue.task_done()

    def run_workload(self, num_inserts, num_updates, num_threads, use_timestamp):
        # Setup table
        self.setup_table()

        # Generate workload
        workload_commands = self.generate_workload(num_inserts, num_updates, use_timestamp)

        # Save workload to file
        today_date = datetime.date.today().strftime("%Y%m%d")
        timestamp_str = "with_timestamp" if use_timestamp else "without_timestamp"
        workload_filename = f"workload_commands_{num_inserts}i_{num_updates}u_{timestamp_str}_{today_date}.txt"
        self.save_workload_to_file(workload_filename, workload_commands)

        # Execute workload
        command_queue = Queue()
        for command in workload_commands:
            command_queue.put(command)

        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=self.worker, args=(command_queue,))
            t.start()
            threads.append(t)

        command_queue.join()

        for t in threads:
            t.join()

        # Generate result.json
        final_data = {"queries_and_times": self.queries_and_times, "traces": []}

        for trace in self.traces_res:
            organized_trace = {
                "query": trace["query"],
                "launched_at": trace["started_at"],
                "coordinator_timestamp": trace.get("coordinator_timestamp"),
                "memtable_timestamp": trace.get("memtable_timestamp"),
                "details": trace["events"],
            }
            final_data["traces"].append(organized_trace)

        result_filename = f"result_{num_inserts}i_{num_updates}u_{timestamp_str}_{today_date}.json"
        with open(result_filename, "w") as file:
            json.dump(final_data, file, ensure_ascii=False, sort_keys=True, default=str, indent=4)

        # Generate conflicts.json
        conflicts = self.find_conflicts(final_data)
        conflicts_filename = f"conflicts_{num_inserts}i_{num_updates}u_{timestamp_str}_{today_date}.json"
        with open(conflicts_filename, "w") as file:
            json.dump(conflicts, file, indent=4)

        # Generate graph
        self.create_enhanced_conflict_marked_graph(final_data, conflicts, f"graph_{num_inserts}i_{num_updates}u_{timestamp_str}_{today_date}.png")

        # Drop the table
        self.session.execute("DROP TABLE IF EXISTS simpletry.person;")

        print("Workload executed and results saved.")

    def extract_queries_and_timestamps(self, data):
        timestamps = []
        
        insert_pattern = re.compile(r"INSERT INTO simpletry.person \(id, name, toTS\) VALUES \('[^']+', '([^']+)', \{'(\d+)':toTimestamp\(now\(\)\)\}\)")
        update_pattern = re.compile(r"UPDATE simpletry.person SET name = '([^']+)', toTS\['(\d+)'\] = toTimestamp\(now\(\)\) WHERE id = '[^']+'")
        
        for query, execution_time in data['queries_and_times']:
            insert_match = insert_pattern.search(query)
            update_match = update_pattern.search(query)
            
            if insert_match:
                name = insert_match.group(1)
                toTS = int(insert_match.group(2))
            elif update_match:
                name = update_match.group(1)
                toTS = int(update_match.group(2))
            else:
                print(f"Warning: No match found for query: {query}")
                continue
            
            trace_timestamps = [
                trace['coordinator_timestamp'] for trace in data['traces'] 
                if trace['query'] == query and trace.get('coordinator_timestamp')
            ]
            if trace_timestamps:
                timestamps.append((name, toTS, trace_timestamps[0]))
            else:
                print(f"Warning: No valid timestamp found for query: {query}")
                timestamps.append((name, toTS, execution_time))  # Use execution time as fallback
        
        return timestamps

    def find_conflicts(self, data):
        timestamps = self.extract_queries_and_timestamps(data)
        conflicts = []
        
        sorted_queries = sorted(timestamps, key=lambda x: int(re.search(r'\d+', x[0]).group()))
        
        for i in range(len(sorted_queries) - 1):
            current_name, current_toTS, current_timestamp = sorted_queries[i]
            next_name, next_toTS, next_timestamp = sorted_queries[i + 1]
            
            if current_timestamp and next_timestamp and current_timestamp > next_timestamp:
                conflict = {
                    'earlier_name': current_name,
                    'earlier_toTS': current_toTS,
                    'earlier_timestamp': current_timestamp,
                    'later_name': next_name,
                    'later_toTS': next_toTS,
                    'later_timestamp': next_timestamp
                }
                conflicts.append(conflict)
        
        return conflicts

    def create_enhanced_conflict_marked_graph(self, data, conflicts, output_image_path):
        timestamps = self.extract_queries_and_timestamps(data)
        conflict_indices_set = {entry["earlier_toTS"] for entry in conflicts}
        
        normal_times = []
        normal_indices = []
        conflict_times = []
        conflict_indices = []
        
        for entry in timestamps:
            index = entry[1]
            timestamp_str = entry[2]
            
            if timestamp_str is None:
                continue
            
            try:
                timestamp = datetime.datetime.fromisoformat(timestamp_str)
            except ValueError:
                print(f"Warning: Unable to parse timestamp {timestamp_str}")
                continue
            
            if index in conflict_indices_set:
                conflict_times.append(timestamp)
                conflict_indices.append(index)
            else:
                normal_times.append(timestamp)
                normal_indices.append(index)
        
        plt.figure(figsize=(14, 8))
        
        plt.scatter(normal_times, normal_indices, color='blue', label='Normal')
        plt.scatter(conflict_times, conflict_indices, color='red', label='Conflict')
        
        plt.xlabel('Time')
        plt.ylabel('Index Number')
        plt.title('Index Number vs Time with Conflict Markings')
        
        plt.gcf().autofmt_xdate()
        plt.xticks(rotation=45)
        
        ax = plt.gca()
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        plt.legend(loc='upper left')
        
        plt.tight_layout()
        
        plt.savefig(output_image_path, format='png')
        plt.close()

if __name__ == "__main__":
    print("Cassandra Workload Generator")
    
    num_inserts = int(input("Enter number of inserts: "))
    num_updates = int(input("Enter number of updates: "))
    num_threads = int(input("Enter number of threads: "))
    use_timestamp = input("Use TIMESTAMP clause for inserts? (y/n): ").lower() == 'y'

    workload = CassandraWorkload()
    workload.run_workload(num_inserts, num_updates, num_threads, use_timestamp)
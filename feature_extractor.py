import csv
import sys
import statistics
import math
import os
from datetime import datetime, timedelta

EXPECTED_CSV_COLUMNS = 8
#CSV COLUMNS
COMMIT_HASH = 0
PARENT_HASHES = 1
AUTHOR_NAME = 2
AUTHOR_EMAIL = 3
AUTHOR_DATE = 4
INTEGRATOR_NAME = 5
INTEGRATOR_EMAIL = 6
INTEGRATOR_DATE = 7

PEAK_UP = 1
PEAK_DOWN = -1
PEAK_NONE = 0


class UniqueDeveloperList:
    def __init__(self):
        self.developers = []
    def add_developer(self, developer_name):
        if developer_name not in self.developers:
            self.developers.append(developer_name)
            return True
        return False
    def count_developers(self):
        return len(self.developers)

class FeatureVector:
    def __init__(self):
        self.duration = 0
        self.max_y = 0
        self.max_y_pos = 0
        self.mean_y = 0.0
        self.sum_y = 0
        self.q25 = None
        self.q50 = None
        self.q75 = None
        self.std_y = None
        self.peak_down = None
        self.peak_none = None
        self.peak_up = None
        self.min_tbp_up = None
        self.avg_tbp_up = None
        self.max_tbp_up = None
        self.min_amplitude = None
        self.avg_amplitude = None
        self.max_amplitude = None
        self.min_ppd = None
        self.avg_ppd = None
        self.max_ppd = None
        self.min_npd = None
        self.avg_npd = None
        self.max_npd = None
        self.min_ps = None
        self.mean_ps = None
        self.max_ps = None
        self.sum_ps = None
        self.min_ns = None
        self.mean_ns = None
        self.max_ns = None
        self.sum_ns = None
        self.min_pg = None
        self.avg_pg = None
        self.max_pg = None
        self.min_ng = None
        self.avg_ng = None
        self.max_ng = None
        self.pg_count = None
        self.ng_count = None
    def to_list(self):
        return [self.duration, self.max_y, self.max_y_pos, self.mean_y, self.sum_y, self.q25, self.q50, self.q75, self.std_y, self.peak_down, self.peak_none, self.peak_up, self.min_tbp_up, self.avg_tbp_up, self.max_tbp_up, self.min_amplitude, self.avg_amplitude, self.max_amplitude, self.min_ppd, self.avg_ppd, self.max_ppd, self.min_npd, self.avg_npd, self.max_npd, self.min_ps, self.mean_ps, self.max_ps, self.sum_ps, self.min_ns, self.mean_ns, self.max_ns, self.sum_ns, self.min_pg, self.avg_pg, self.max_pg, self.min_ng, self.avg_ng, self.max_ng, self.pg_count, self.ng_count]
    def keys_order(self):
        return ["duration", "max_y", "max_y_pos", "mean_y", "sum_y", "q25", "q50", "q75", "std_y", "peak_down", "peak_none", "peak_up", "min_tbp_up", "avg_tbp_up", "max_tbp_up", "min_amplitude", "avg_amplitude", "max_amplitude", "min_ppd", "avg_ppd", "max_ppd", "min_npd", "avg_npd", "max_npd", "min_ps", "mean_ps", "max_ps", "sum_ps", "min_ns", "mean_ns", "max_ns", "sum_ns", "min_pg", "avg_pg", "max_pg", "min_ng", "avg_ng", "max_ng", "pg_count", "ng_count"]

def extract_all_measures_from_file(log_file_path, time_series_file_path):
    line_num = 0

    earliest_intergration_date = sys.maxsize
    latest_integration_date = 0
    earliest_author_date = sys.maxsize
    latest_author_date = 0

    with open(log_file_path, 'r', encoding="utf-8") as file:
        csv_file_reader = csv.reader(file, delimiter=',', quotechar='"')
        for commit in csv_file_reader:
            line_num += 1
            if len(commit) != EXPECTED_CSV_COLUMNS:
                raise ("line %i has an incorrect number of columns" % line_num)
            current_integration_date = int(commit[INTEGRATOR_DATE])
            earliest_intergration_date = min([current_integration_date, earliest_intergration_date])
            latest_integration_date = max([current_integration_date, latest_integration_date])
            current_author_date = int(commit[AUTHOR_DATE])
            earliest_author_date = min([current_author_date, earliest_author_date])
            latest_author_date = max([current_author_date, latest_author_date])

    earliest_intergration_date = get_monday_timestamp(earliest_intergration_date)
    latest_integration_date = get_monday_timestamp(latest_integration_date)
    earliest_author_date = get_monday_timestamp(earliest_author_date)
    latest_author_date = get_monday_timestamp(latest_author_date)

    total_weeks_integration = calculate_week_num(earliest_intergration_date, latest_integration_date) + 1
    total_weeks_commits = calculate_week_num(earliest_author_date, latest_author_date) + 1
    
    integration_frequency_timeseries = [0]*total_weeks_integration

    integrator_activity_count = [UniqueDeveloperList() for _ in range(total_weeks_integration)]
    integrator_activity_timeseries = [0]*total_weeks_integration

    commit_frequency_timeseries = [0]*total_weeks_commits
    author_activity_count = [UniqueDeveloperList() for _ in range(total_weeks_commits)]
    author_activity_timeseries = [0]*total_weeks_commits

    merge_frequency_timeseries = [0]*total_weeks_integration


    with open(log_file_path, 'r', encoding="utf-8") as file:
        csv_file_reader = csv.reader(file, delimiter=',', quotechar='"')
        for commit in csv_file_reader:

            current_integration_date = commit[INTEGRATOR_DATE]
            integration_week_number = calculate_week_num(earliest_intergration_date, current_integration_date)

            current_author_date = commit[AUTHOR_DATE]
            committer_week_number = calculate_week_num(earliest_author_date, current_author_date)

            integration_frequency_timeseries[integration_week_number] += 1

            if (integrator_activity_count[integration_week_number].add_developer(commit[INTEGRATOR_EMAIL])):
                integrator_activity_timeseries[integration_week_number] += 1
            
            commit_frequency_timeseries[committer_week_number] += 1

            if (author_activity_count[committer_week_number].add_developer(commit[AUTHOR_EMAIL])):
                author_activity_timeseries[committer_week_number] += 1

            if " " in commit[PARENT_HASHES]:
                merge_frequency_timeseries[integration_week_number] += 1

    week_timestamps = []
    current_timestamp = get_monday_timestamp(earliest_author_date)
    for i in range(total_weeks_commits):
        current_timestamp = (datetime.fromtimestamp(current_timestamp) + timedelta(days=7)).timestamp()
        week_timestamps.append(current_timestamp-1)

    with open(time_series_file_path, "w") as file:
        csv_writer = csv.writer(file, quoting=csv.QUOTE_NONE)
        csv_writer.writerow(['filename','date','merges','commits','integrations','commiters','integrators'])
        for i, _ in enumerate(integration_frequency_timeseries):
            csv_writer.writerow([os.path.basename(log_file_path), 
                                datetime.fromtimestamp(week_timestamps[i]).strftime("%Y-%m-%d"), 
                                merge_frequency_timeseries[i],
                                commit_frequency_timeseries[i],
                                integration_frequency_timeseries[i],
                                author_activity_timeseries[i],
                                integrator_activity_timeseries[i],
                                ])

    integration_frequency_feature_vector = calculate_feature_vector_from_time_series(integration_frequency_timeseries)
    integrator_activity_feature_vector = calculate_feature_vector_from_time_series(integrator_activity_timeseries)
    commit_frequency_feature_vector = calculate_feature_vector_from_time_series(commit_frequency_timeseries)
    author_activity_feature_vector = calculate_feature_vector_from_time_series(author_activity_timeseries)
    merge_frequency_feature_vector = calculate_feature_vector_from_time_series(merge_frequency_timeseries)
    return {
        "integrations":integration_frequency_feature_vector, 
        "integrators":integrator_activity_feature_vector, 
        "commits":commit_frequency_feature_vector, 
        "authors":author_activity_feature_vector, 
        "merges":merge_frequency_feature_vector
    }

def calculate_feature_vector_from_time_series(timeseries):
    features = FeatureVector()
    features.duration = len(timeseries)
    features.max_y = 0
    features.max_y_pos = 0
    current_week_index = 0
    for current_value in timeseries:
        if current_value > features.max_y:
            features.max_y = current_value
            features.max_y_pos = current_week_index
        features.sum_y += current_value
    features.mean_y = features.sum_y / features.duration
    if (len(timeseries) > 2): # < 2 occurs when there is only one week
        features.std_y = statistics.stdev(timeseries) #standard deviation

    #peaks is unused, but may be helpful in the future for graphing
    (peaks, features) = detect_peaks_and_set_features(timeseries, features)

    return features

def detect_peaks_and_set_features(data_set, features):
    return_vector = [PEAK_NONE]*len(data_set)

    previous_gradient_value = 0.0

    positive_gradients = []
    negative_gradients = []

    if len(data_set) <= 1:
        return (return_vector, features)
    
    peak_up = 0
    peak_down = 0
    peak_none = len(data_set)

    current_seq = 0

    ps_sequence = []
    ns_sequence = []

    mean = statistics.mean(data_set)

    positive_deviations = []
    negative_deviations = []

    last_peak_up = 0
    last_peak_down = 0

    time_between_peaks_up = []
    time_between_peaks_down = []

    time_between_peaks_up = []
    time_between_peaks_down = []

    amplitudes = []

    index = 1
    array_length = len(data_set)
    upward_trend = False
    downward_trend = False
    last_peak_down_value = data_set[0]
    while index < array_length:
        previous = data_set[index-1]
        current = data_set[index]

        if previous < current:
            current_seq += 1
            upward_trend = True
            if (downward_trend):
                return_vector[index-1] = PEAK_DOWN
                last_peak_down_value = previous
                peak_down += 1
                peak_none -= 1
                ns_sequence.append(current_seq)
                negative_deviations.append(previous - mean)
                negative_gradients.append(current - previous_gradient_value)
                previous_gradient_value = current
                current_seq = 0
                time_between_peaks_down.append(index - last_peak_down)
                last_peak_down = index
                downward_trend = False
        if previous > current:
            downward_trend = True
            if upward_trend:
                amplitudes.append(abs((previous - last_peak_down_value) / features.max_y))
                return_vector[index - 1] = PEAK_UP
                positive_deviations.append(previous - mean)
                peak_up += 1
                peak_none -= 1
                ps_sequence.append(current_seq)
                positive_gradients.append(current - previous_gradient_value)
                #positive_gradient_value = current
                current_seq = 0
                time_between_peaks_up.append(index - last_peak_up)
                last_peak_up = index
                upward_trend = False
        index += 1
    if upward_trend:
        ns_sequence.append(current_seq)
    else:
        ps_sequence.append(current_seq)

    features.pg_count = len(positive_gradients)
    features.ng_count = len(negative_gradients)

    features.min_pg = min(positive_gradients)
    features.avg_pg = statistics.mean(positive_gradients)
    features.max_pg = max(positive_gradients)

    features.min_ng = min(negative_gradients)
    features.avg_ng = statistics.mean(negative_gradients)
    features.max_ng = max(negative_gradients)

    features.peak_up = peak_up
    features.peak_down = peak_down
    features.peak_none = peak_none

    features.min_ps = min(ps_sequence)
    features.mean_ps = statistics.mean(ps_sequence)
    features.max_ps = max(ps_sequence)
    features.sum_ps = sum(ps_sequence)

    features.min_ns = min(ns_sequence)
    features.mean_ns = statistics.mean(ns_sequence)
    features.max_ns = max(ns_sequence)
    features.sum_ns = sum(ns_sequence)
    
    features.min_ppd = min(positive_deviations)
    features.avg_ppd = statistics.mean(positive_deviations)
    features.max_ppd = max(positive_deviations)

    features.min_npd = min(negative_deviations)
    features.avg_npd = statistics.mean(negative_deviations)
    features.max_npd = max(negative_deviations)

    features.pg_count = len(positive_gradients)
    features.ng_count = len(negative_gradients)

    features.min_pg = min(positive_gradients)
    features.avg_pg = statistics.mean(positive_gradients)
    features.max_pg = max(positive_gradients)

    features.min_ng = min(negative_gradients)
    features.avg_ng = statistics.mean(negative_gradients)
    features.max_ng = max(negative_gradients)
    
    features.min_tbp_up = min(time_between_peaks_up)
    features.avg_tbp_up = statistics.mean(time_between_peaks_up)
    features.max_tbp_up = max(time_between_peaks_up)

    #TODO: features,min_tbp_down

    features.min_amplitude = min(amplitudes)
    features.avg_amplitude = statistics.mean(amplitudes)
    features.max_amplitude = max(amplitudes)

    data_set = sorted(data_set)
    features.q25 = find_quantile(data_set, 0.25)
    features.q50 = find_quantile(data_set, 0.5)
    features.q75 = find_quantile(data_set, 0.75)

    return (return_vector, features)

def find_quantile(data_set, quantile):
    length = len(data_set) - 1
    upper_index = math.ceil(length*quantile)
    lower_index = math.floor(length*quantile)
    upper_value = data_set[upper_index]
    lower_value = data_set[lower_index]
    return (upper_value + lower_value) / 2

def get_monday_timestamp(timestamp):
    current_date = datetime.fromtimestamp(timestamp)
    current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0) #reset the seconds to 0
    current_date = current_date - timedelta(days=current_date.weekday()) #gives you monday
    return current_date.timestamp()

def calculate_week_num(base_time, week_time):
    base_datetime = datetime.fromtimestamp(int(base_time))
    week_datetime = datetime.fromtimestamp(int(week_time)) 

    return (week_datetime - base_datetime).days // 7

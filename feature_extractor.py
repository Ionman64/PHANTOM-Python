import csv
import sys
import statistics
import math
import os
from datetime import datetime, timedelta, timezone
import inspect

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
        self.max_y = 0 #Not Implemented
        self.max_y_pos = 0
        self.mean_y = 0.0
        self.sum_y = 0
        self.q25 = None
        self.q50 = None
        self.q75 = None
        self.std_y = None #Not Implemented
        self.peak_down = None
        self.peak_none = None
        self.peak_up = None
        self.min_tbp_up = None
        self.avg_tbp_up = None
        self.max_tbp_up = None

        self.avg_tbp_down = None #Not Implemented
        self.max_tbp_down = None #Not Implemented
        self.min_tbp_down = None #Not Implemented

        self.min_amp = None #Not Implemented
        self.avg_amp = None #Not Implemented
        self.max_amp = None #Not Implemented
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
    def to_dict(self):
        return {
            "duration":self.duration, 
            "max_y":self.max_y, 
            "max_y_pos":self.max_y_pos, 
            "mean_y":self.mean_y, 
            "sum_y":self.sum_y, 
            "q25":self.q25, 
            "q50":self.q50, 
            "q75":self.q75, 
            "std":self.std_y, 
            "peak_down":self.peak_down, 
            "peak_none":self.peak_none, 
            "peak_up":self.peak_up, 
            "min_TBP_up":self.min_tbp_up, 
            "avg_TBP_up":self.avg_tbp_up, 
            "max_TBP_up":self.max_tbp_up, 
            "min_TBP_down":self.min_tbp_down,
            "max_TBP_down":self.max_tbp_down,
            "avg_TBP_down":self.avg_tbp_down,
            "min_amp":self.min_amp, 
            "avg_amp":self.avg_amp, 
            "max_amp":self.max_amp, 
            "min_PPD":self.min_ppd, 
            "avg_PPD":self.avg_ppd, 
            "max_PPD":self.max_ppd, 
            "min_NPD":self.min_npd, 
            "avg_NPD":self.avg_npd, 
            "max_NPD":self.max_npd, 
            "min_PS":self.min_ps, 
            "mean_PS":self.mean_ps, 
            "max_PS":self.max_ps, 
            "sum_PS":self.sum_ps, 
            "min_NS":self.min_ns, 
            "mean_NS":self.mean_ns, 
            "max_NS":self.max_ns, 
            "sum_NS":self.sum_ns, 
            "min_PG":self.min_pg, 
            "avg_PG":self.avg_pg, 
            "max_PG":self.max_pg, 
            "min_NG":self.min_ng, 
            "avg_NG":self.avg_ng, 
            "max_NG":self.max_ng, 
            "PG_count":self.pg_count, 
            "NG_count":self.ng_count
            }
    def keys_order(self):
        return sorted(self.to_dict().keys())

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
                raise BadCSVFormat("line %i has an incorrect number of columns" % line_num)
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
    
    integration_frequency_timeseries = prefilled_array(0, total_weeks_integration)

    integrator_activity_count = prefilled_array(UniqueDeveloperList, total_weeks_integration)
    integrator_activity_timeseries = prefilled_array(0, total_weeks_integration)

    commit_frequency_timeseries = prefilled_array(0, total_weeks_commits)
    author_activity_count = prefilled_array(UniqueDeveloperList, total_weeks_commits)
    author_activity_timeseries = prefilled_array(0, total_weeks_commits)

    merge_frequency_timeseries = prefilled_array(0, total_weeks_integration)

    line_number = 0
    with open(log_file_path, 'r', encoding="utf-8") as file:
        csv_file_reader = csv.reader(file, quoting=csv.QUOTE_NONE, delimiter=',')
        for (line_num, commit) in enumerate(csv_file_reader):
            current_integration_date = int(commit[INTEGRATOR_DATE])
            integration_week_number = calculate_week_num(earliest_intergration_date, current_integration_date)

            current_author_date = int(commit[AUTHOR_DATE])
            committer_week_number = calculate_week_num(earliest_author_date, current_author_date)

            integration_frequency_timeseries[integration_week_number] += 1

            if integrator_activity_count[integration_week_number].add_developer(commit[INTEGRATOR_EMAIL]):
                integrator_activity_timeseries[integration_week_number] += 1
            
            commit_frequency_timeseries[committer_week_number] += 1

            if author_activity_count[committer_week_number].add_developer(commit[AUTHOR_EMAIL]):
                author_activity_timeseries[committer_week_number] += 1

            if " " in commit[PARENT_HASHES]:
                merge_frequency_timeseries[integration_week_number] += 1
            line_number += 1
    print ("Read %s line(s) in %s" % (line_number, log_file_path))

    if time_series_file_path is not None:
        week_timestamps = []
        current_timestamp = get_monday_timestamp(earliest_author_date)
        for i in range(total_weeks_commits):
            current_timestamp = (datetime.fromtimestamp(current_timestamp) + timedelta(days=7)).timestamp()
            week_timestamps.append(current_timestamp-1)

        with open(time_series_file_path, "w") as file:
            csv_writer = csv.writer(file, quoting=csv.QUOTE_NONE, lineterminator='\n')
            csv_writer.writerow(['filename','date','merges','commits','integrations','commiters','integrators'])
            for i, _ in enumerate(integration_frequency_timeseries):
                csv_writer.writerow([os.path.basename(log_file_path), 
                                    datetime.fromtimestamp(week_timestamps[i]).astimezone(timezone.utc).strftime("%Y-%m-%d"), 
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
    return ({
        "integrations":integration_frequency_feature_vector, 
        "integrators":integrator_activity_feature_vector, 
        "commits":commit_frequency_feature_vector, 
        "authors":author_activity_feature_vector, 
        "merges":merge_frequency_feature_vector
    },{
        "integrations_ts":integration_frequency_timeseries, 
        "integrators_ts":integrator_activity_timeseries, 
        "commits_ts":commit_frequency_timeseries, 
        "authors_ts":author_activity_timeseries, 
        "merges_ts":merge_frequency_timeseries
    })

def calculate_feature_vector_from_time_series(timeseries):
    features = FeatureVector()
    features.duration = len(timeseries)
    features.max_y = 0
    features.max_y_pos = 0
    for (current_week_index, current_value) in enumerate(timeseries):
        if current_value > features.max_y:
            features.max_y = current_value
            features.max_y_pos = current_week_index + 1 #This +1 op could be the source of some issues, it is basically because the output starts from 1 and not 0
        current_week_index += 1
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
    previous_gradient_value = data_set[0] #FIX
    while index < array_length:
        previous = data_set[index-1]
        current = data_set[index]

        if previous < current:
            current_seq += 1
            upward_trend = True
            if downward_trend:
                return_vector[index-1] = PEAK_DOWN
                last_peak_down_value = previous
                peak_down += 1
                peak_none -= 1
                ns_sequence.append(current_seq)
                negative_deviations.append(previous - mean)
                current_seq = 0
                time_between_peaks_down.append(index - last_peak_down)
                last_peak_down = index
                downward_trend = False
            positive_gradients.append(current - previous_gradient_value)
           
        if previous > current:
            current_seq += 1
            downward_trend = True
            if upward_trend:
                amplitudes.append(abs((previous - last_peak_down_value) / features.max_y))
                return_vector[index - 1] = PEAK_UP
                positive_deviations.append(previous - mean)
                peak_up += 1
                peak_none -= 1
                ps_sequence.append(current_seq)
                current_seq = 0
                time_between_peaks_up.append(index - last_peak_up)
                last_peak_up = index
                upward_trend = False
            negative_gradients.append(current - previous_gradient_value)
            
        previous_gradient_value = current 
        index += 1
    if upward_trend:
        ns_sequence.append(current_seq)
    else:
        ps_sequence.append(current_seq)

    features.pg_count = len(positive_gradients)
    features.ng_count = len(negative_gradients)

    if len(positive_gradients) > 0:
        features.min_pg = min(positive_gradients)
        features.avg_pg = statistics.mean(positive_gradients)
        features.max_pg = max(positive_gradients)
        
        features.pg_count = len(positive_gradients)

        features.min_pg = min(positive_gradients)
        features.avg_pg = statistics.mean(positive_gradients)
        features.max_pg = max(positive_gradients)
    else:
        features.min_pg = 0
        features.avg_pg = 0
        features.max_pg = 0
        
        features.pg_count = 0

        features.min_pg = 0
        features.avg_pg = 0
        features.max_pg = 0
    
    if len(negative_gradients) > 0:
        features.min_ng = min(negative_gradients)
        features.avg_ng = statistics.mean(negative_gradients)
        features.max_ng = max(negative_gradients)

        features.ng_count = len(negative_gradients)

        features.min_ng = min(negative_gradients)
        features.avg_ng = statistics.mean(negative_gradients)
        features.max_ng = max(negative_gradients)
    else:
        features.min_ng = 0
        features.avg_ng = 0
        features.max_ng = 0
        
        features.ng_count = 0

        features.min_ng = 0
        features.avg_ng = 0
        features.max_ng = 0

    features.peak_up = peak_up
    features.peak_down = peak_down
    features.peak_none = peak_none

    if len(ps_sequence) > 0:
        features.min_ps = min(ps_sequence)
        features.mean_ps = statistics.mean(ps_sequence)
        features.max_ps = max(ps_sequence)
        features.sum_ps = sum(ps_sequence)
    else:
        features.min_ps = 0
        features.mean_ps = 0
        features.max_ps = 0
        features.sum_ps = 0

    if len(ns_sequence) > 0:
        features.min_ns = min(ns_sequence)
        features.mean_ns = statistics.mean(ns_sequence)
        features.max_ns = max(ns_sequence)
        features.sum_ns = sum(ns_sequence)
    else:
        features.min_ns = 0
        features.mean_ns = 0
        features.max_ns = 0
        features.sum_ns = 0
    
    if len(positive_deviations) > 0:
        features.min_ppd = min(positive_deviations)
        features.avg_ppd = statistics.mean(positive_deviations)
        features.max_ppd = max(positive_deviations)
    else:
        features.min_ppd = 0
        features.avg_ppd = 0
        features.max_ppd = 0

    if len(negative_deviations) > 0:
        features.min_npd = min(negative_deviations)
        features.avg_npd = statistics.mean(negative_deviations)
        features.max_npd = max(negative_deviations)
    else:
        features.min_npd = 0
        features.avg_npd = 0
        features.max_npd = 0
    
    if len(time_between_peaks_up) > 0:
        features.min_tbp_up = min(time_between_peaks_up)
        features.avg_tbp_up = statistics.mean(time_between_peaks_up)
        features.max_tbp_up = max(time_between_peaks_up)
    else:
        features.min_tbp_up = 0
        features.avg_tbp_up = 0
        features.max_tbp_up = 0

    if len(time_between_peaks_down) > 0:
        features.min_tbp_down = min(time_between_peaks_down)
        features.avg_tbp_down = statistics.mean(time_between_peaks_down)
        features.max_tbp_down = max(time_between_peaks_down)
    else:
        features.min_tbp_down = 0
        features.avg_tbp_up = 0
        features.max_tbp_down = 0

    if len(amplitudes) > 0:
        features.min_amp = min(amplitudes)
        features.avg_amp = statistics.mean(amplitudes)
        features.max_amp = max(amplitudes)
    else:
        features.min_amp = 0
        features.avg_amp = 0
        features.max_amp = 0

    data_set = sorted(data_set)
    features.q25 = find_quantile(data_set, 0.25)
    features.q50 = find_quantile(data_set, 0.5)
    features.q75 = find_quantile(data_set, 0.75)

    return (return_vector, features)

def find_quantile(data_set, quantile):
    if quantile > 1 or quantile < 0:
        raise InvalidParam("Quantile must be between 0.00 and 1.00")
    arr = sorted(data_set)
    length = len(arr) - 1
    upper_index = math.ceil(length*quantile)
    lower_index = math.floor(length*quantile)
    upper_value = arr[upper_index]
    lower_value = arr[lower_index]
    return (upper_value + lower_value) / 2

def get_monday_timestamp(timestamp):
    #gives the timestamp at midnight on the nearest monday BEFORE the provided timestamp
    SECONDS_IN_A_DAY = 86400
    current_date = datetime.utcfromtimestamp(timestamp)
    return int(current_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp()) - (current_date.weekday()*SECONDS_IN_A_DAY+2*3600)

def calculate_week_num(base_time, week_time):
    #calculates the number of weeks after the base time 
    if base_time > week_time:
        raise InvalidParam("Base time cannot be higher than the week time")
    SECS_IN_WEEK = 604800
    return int(abs((week_time - base_time)) / SECS_IN_WEEK)

def prefilled_array(fill_with, size):
    #produces an array with the 'fill_with' parameter, if it's a class it will instantiate the class for each index before returning
    if inspect.isclass(fill_with):
        return [fill_with() for _ in range(size)]
    return [fill_with]*size

#Errors

class BadCSVFormat(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message

class InvalidParam(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message
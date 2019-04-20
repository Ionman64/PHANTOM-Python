import unittest
import feature_extractor
import random
import numpy as np
import os
import pandas as pd

class FakeObjectForTesting():
    def __init__(self):
        self.a = "a"
        self.b = "b"
        self.random_value = random.random()
    def get_a(self):
        return self.a
    def get_b(self):
        return self.b
    
class PreFilledArray(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_prefilled_array_test_with_0(self):
        self.assertEqual([0,0,0,0,0], feature_extractor.prefilled_array(0, 5))

    def test_prefilled_array_test_with_class(self):
        array = feature_extractor.prefilled_array(FakeObjectForTesting, 5)
        previous = None
        for current in array:
            if previous is current:
                #Object is the same as the previous
                assert False
            else:
                previous = current
        assert True

    def test_prefilled_array_test_with_object(self):
        fake_object = FakeObjectForTesting()
        self.assertEqual([fake_object]*5, feature_extractor.prefilled_array(fake_object, 5))

    def test_prefilled_array_test_with_string(self):
        self.assertEqual(["Hello", "Hello", "Hello", "Hello", "Hello"], feature_extractor.prefilled_array("Hello", 5))

class GetMondayTimeStamp(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_get_monday_timestamp_1(self):
        test_timestamp = 1555423748 #Tue, 16 Apr 2019 14:10:32
        test_monday_timestamp = 1555286400 #Mon, 15 Apr 2019 00:00:00
        self.assertEqual(test_monday_timestamp, feature_extractor.get_monday_timestamp(test_timestamp))
    def test_get_monday_timestamp_2(self):
        test_timestamp = 1456747200 #Mon, 29 Feb 2016 12:00:00
        test_monday_timestamp = 1456704000 #Mon, 29 Feb 2016 00:00:00
        self.assertEqual(test_monday_timestamp, feature_extractor.get_monday_timestamp(test_timestamp))
    def test_get_monday_timestamp_3(self):
        test_timestamp = 1556495999 #Sun, 28 Apr 2019 23:59:59
        test_monday_timestamp = 1555891200 #Mon, 22 Apr 2019 00:00:00
        self.assertEqual(test_monday_timestamp, feature_extractor.get_monday_timestamp(test_timestamp))

class CalculateWeekNumber(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_get_week_number_1(self):
        test_base_timestamp = 1456704000
        test_week_timestamp = 1555891200
        test_week_number = 164
        self.assertEqual(test_week_number, feature_extractor.calculate_week_num(test_base_timestamp, test_week_timestamp))
    def test_get_week_number_2(self):
        test_base_timestamp = 1555286400
        test_week_timestamp = 1555891200
        test_week_number = 1
        self.assertEqual(test_week_number, feature_extractor.calculate_week_num(test_base_timestamp, test_week_timestamp))
    def test_get_week_number_3(self):
        #Given the same result the week will calculate 0
        test_base_timestamp = 1555200000 
        test_week_timestamp = 1555200001
        test_week_number = 0
        self.assertEqual(test_week_number, feature_extractor.calculate_week_num(test_base_timestamp, test_week_timestamp))
    def test_get_week_number_4(self):
        #Given the same result the week will calculate 0
        test_base_timestamp = 2 
        test_week_timestamp = 1
        self.assertRaises(feature_extractor.InvalidParam, feature_extractor.calculate_week_num, test_base_timestamp, test_week_timestamp)

class FindQuantile(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_find_quantile(self):
        test_arr = [1,2,3,4,5,6,7,8,9,10]
        self.assertEqual(3.5, feature_extractor.find_quantile(test_arr, 0.25))
        self.assertEqual(5.5, feature_extractor.find_quantile(test_arr, 0.5))
        self.assertEqual(7.5, feature_extractor.find_quantile(test_arr, 0.75))
    def test_find_quantile_compare_numpy(self):
        test_arr = [1,2,3,4,5,6,7,8,9,10]
        self.assertEqual(np.percentile(test_arr, 25, interpolation='midpoint'), 
            feature_extractor.find_quantile(test_arr, 0.25))
        self.assertEqual(np.percentile(test_arr, 50, interpolation='midpoint'), 
            feature_extractor.find_quantile(test_arr, 0.5))
        self.assertEqual(np.percentile(test_arr, 75, interpolation='midpoint'), 
            feature_extractor.find_quantile(test_arr, 0.75))
    def test_find_quantile_2(self):
        test_arr = [1,2,3,4,5,6,7,8,9,10]
        self.assertRaises(feature_extractor.InvalidParam, feature_extractor.find_quantile, test_arr, 1.01)
        self.assertRaises(feature_extractor.InvalidParam, feature_extractor.find_quantile, test_arr, 2)
        self.assertRaises(feature_extractor.InvalidParam, feature_extractor.find_quantile, test_arr, -0.01)
        self.assertRaises(feature_extractor.InvalidParam, feature_extractor.find_quantile, test_arr, -1)
    def test_find_quantile_3(self):
        test_arr = [1,1,1,1,1,1,1,1,1,1]
        self.assertEqual(1, feature_extractor.find_quantile(test_arr, 0.1))
        self.assertEqual(1, feature_extractor.find_quantile(test_arr, 0.9))
        self.assertEqual(1, feature_extractor.find_quantile(test_arr, 1))
        self.assertEqual(1, feature_extractor.find_quantile(test_arr, 0))

class FeaturesTest(unittest.TestCase):
    oracles = dict()
    
    def setUp(self):
        self.oracles['FFmpeg'] = pd.read_csv('./test_data/FFmpeg_FFmpeg-features.csv')
        self.oracles['py-ccflex'] = pd.read_csv('./test_data/py-ccflex-features.csv')
    
    def tearDown(self):
        pass
    
    def feature_oracle(self, project, measure, feature):
        if project not in self.oracles.keys():
            raise Exception(f"Features for {project} were not loaded!")
        return self.oracles[project][self.oracles[project]['measure'] == measure ][feature].values[0]
    
    def test_feature_extraction_ffmpeg_integrations(self):
        fv_obj = feature_extractor.extract_all_measures_from_file("test_data" + os.sep + "FFmpeg_FFmpeg.log", None)[0]["integrations"]
        fv = fv_obj.to_dict()
        measure = 'integrations'
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'duration'), fv["duration"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'sum_y'), fv["sum_y"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'max_y_pos'), fv["max_y_pos"])
        #self.assertEqual(self.feature_oracle('FFmpeg', measure,'max_y'), fv["max_y"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'PG_count'), fv["PG_count"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'NG_count'), fv["NG_count"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'avg_NG'), fv["avg_NG"])
    
    def test_feature_extraction_ffmpeg_merges(self):
        fv_obj = feature_extractor.extract_all_measures_from_file("test_data" + os.sep + "FFmpeg_FFmpeg.log", None)[0]["merges"]
        fv = fv_obj.to_dict()
        measure = 'merges'
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'duration'), fv["duration"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'sum_y'), fv["sum_y"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'max_y_pos'), fv["max_y_pos"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'PG_count'), fv["PG_count"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'NG_count'), fv["NG_count"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'max_y'), fv["max_y"])
        self.assertEqual(self.feature_oracle('FFmpeg', measure,'avg_NG'), fv["avg_NG"])
    
    def test_feature_extraction_pyccflex_integrations(self):
        fv_obj = feature_extractor.extract_all_measures_from_file("test_data" + os.sep + "py-ccflex.log", None)[0]["integrations"]
        fv = fv_obj.to_dict()
        measure = 'integrations'
        self.assertEqual(self.feature_oracle('py-ccflex', measure,'duration'), fv["duration"])
        self.assertEqual(self.feature_oracle('py-ccflex', measure,'sum_y'), fv["sum_y"])
        self.assertEqual(self.feature_oracle('py-ccflex', measure,'max_y_pos'), fv["max_y_pos"])
        self.assertEqual(self.feature_oracle('py-ccflex', measure,'max_y'), fv["max_y"])
        self.assertEqual(self.feature_oracle('py-ccflex', measure,'PG_count'), fv["PG_count"])
        self.assertEqual(self.feature_oracle('py-ccflex', measure,'NG_count'), fv["NG_count"])
        self.assertEqual(self.feature_oracle('py-ccflex', measure,'avg_NG'), fv["avg_NG"])

    def test_feature_extraction_mysql(self):
        fv_obj = feature_extractor.extract_all_measures_from_file("test_data" + os.sep + "mysql_mysql-server.log", None)[0]["integrations"]
        fv = fv_obj.to_dict()
        self.assertEqual(923, fv["duration"])
        self.assertEqual(140096, fv["sum_y"])
        self.assertEqual(347, fv["max_y_pos"])
        self.assertEqual(485, fv["max_y"])
        self.assertEqual(485, fv["PG_count"])
        self.assertEqual(437, fv["NG_count"])
        self.assertEqual(-46.2173913043478, fv["avg_NG"])


class TimeSeriesTest(unittest.TestCase):
    oracles = dict()
    
    def setUp(self):
        self.oracles['FFmpeg'] = pd.read_csv('./test_data/FFmpeg_FFmpeg-ts.csv')
    
    def tearDown(self):
        pass
    
    def test_ffmpeg_integrations(self):
        res_list = feature_extractor.extract_all_measures_from_file(
            "test_data" + os.sep + "FFmpeg_FFmpeg.log", None)[1]["integrations_ts"]
        oracle_ts = self.oracles['FFmpeg']['integrations'].tolist()
        self.assertListEqual(res_list, oracle_ts)

    def test_ffmpeg_merges(self):
        res_list = feature_extractor.extract_all_measures_from_file(
            "test_data" + os.sep + "FFmpeg_FFmpeg.log", None)[1]["merges_ts"]
        oracle_ts = self.oracles['FFmpeg']['merges'].tolist()
        self.assertListEqual(res_list, oracle_ts)

if __name__ == "__main__":
    unittest.main() # run all tests
else:
    print ("***Warning***: you seem to have imported the test suite, which should not be run on it's own")


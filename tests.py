import unittest
import feature_extractor
import random

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

if __name__ == "__main__":
    unittest.main() # run all tests
else:
    print ("***Warning***: you seem to have imported the test suite, which should not be run on it's own")


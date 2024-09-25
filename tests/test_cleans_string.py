
import sys
import os
sys.path.append(os.path.abspath('/opt/airflow/jobs'))

import unittest
from silver_ingestion import clean_string

class TestSilverIngestion(unittest.TestCase):

    def test_clean_string(self):
        # Testing accents
        input_str = "São Paulo"
        expected_output = "Sao Paulo"
        self.assertEqual(clean_string(input_str), expected_output)
        
        # Testing special characteres
        input_str = "Tóquio@#"
        expected_output = "Toquio__"
        self.assertEqual(clean_string(input_str), expected_output)
        
        # Testing empty string
        input_str = ""
        expected_output = ""
        self.assertEqual(clean_string(input_str), expected_output)
        
        # Testing None value
        input_str = None
        self.assertEqual(clean_string(input_str), input_str)

if __name__ == '__main__':
    unittest.main()

#python -m unittest jobs.test_clean_string

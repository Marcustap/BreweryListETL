import unittest

def run_tests():
    test_dir = '/opt/airflow/tests'
    
    loader = unittest.TestLoader()
    suite = loader.discover(test_dir)

    runner = unittest.TextTestRunner()
    result = runner.run(suite)

    if result.wasSuccessful():
        return 0
    else:
        return 1

if __name__ == "__main__":
    exit(run_tests())
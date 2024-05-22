def compare_version(version1, version2):
    '''
    accepts two version strings and returns whether one is greater than, equal, or less than the other.
    As an example: “1.2” is greater than “1.1”
    '''
    v1Split = version1.split('.')
    v2Split = version2.split('.')
    maxLen = max(len(v1Split), len(v2Split))
    v1Split += [0] * (maxLen - len(v1Split))
    v2Split += [0] * (maxLen - len(v2Split))
    for i in range(maxLen):
        if v1Split[i] > v2Split[i]:
            return 1
        elif v1Split[i] < v2Split[i]:
            return -1
    return 0

import unittest

class TestCompareVersion(unittest.TestCase):
    def test_equal_versions(self):  # test case 1: equal versions
        self.assertEqual(compare_version('1.1', '1.1'), 0)

    def test_greater_version(self):  # test case 2: greater version
        self.assertEqual(compare_version('1.2', '1.1'), 1)

    def test_lesser_version(self):  # test case 3: lesser version
        self.assertEqual(compare_version('1.1', '1.2'), -1)

    def test_different_length_versions(self):  # test case 4: versions with different length
        self.assertEqual(compare_version('1.1.1', '1.1'), 1)
        self.assertEqual(compare_version('1.1', '1.1.1'), -1)

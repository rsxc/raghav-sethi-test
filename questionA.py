def lines_overlap(line1, line2):
    '''
    accepts two lines (x1,x2) and (x3,x4) on the x-axis and returns whether they overlap.
    As an example, (1,5) and (2,6) overlaps but not (1,5) and (6,8)
    --1-2-----5-6-----
    --1-------5-6---8-
    '''
    x1, x2 = line1
    x3, x4 = line2

    return x1 <= x3 <= x2 or x3 <= x1 <= x4

import unittest

class TestLinesOverlap(unittest.TestCase):

    def test_overlapping_lines(self):  # test case 1: overlapping lines
        # --1-2-----5-6-----
        self.assertTrue(lines_overlap((1, 5), (2, 6)))

    def test_non_overlapping_lines(self):  # test case 2: non-overlapping lines
        # --1-------5-6---8-
        self.assertFalse(lines_overlap((1, 5), (6, 8)))

    def test_non_overlapping_lines2(self):  # test case 3: non-overlapping lines
        # --1-------5-6---8-
        self.assertFalse(lines_overlap((6, 8), (1, 5)))

    def test_horizontal_lines(self):  # test case 3: horizontal lines
        # --1-------5-------
        self.assertTrue(lines_overlap((1, 5), (1, 1)))

    def test_equal_lines(self):  # test case 5: equal lines
        # --1-------5-------
        self.assertTrue(lines_overlap((1, 5), (1, 5)))

    def test_lines_with_equal_endpoints(self):  # test case 6: lines with equal endpoints
        # --1-------5-------
        self.assertTrue(lines_overlap((1, 5), (5, 1)))

    def test_lines_with_equal_starting_points(self):  # test case 7: lines with equal starting points
        # --1-------5-6-----
        self.assertTrue(lines_overlap((1, 5), (1, 6)))

    def test_lines_with_equal_ending_points(self):  # test case 8: lines with equal ending points
        # --1-----4-5-------
        self.assertTrue(lines_overlap((1, 5), (4, 5)))


if __name__ == '__main__':
    unittest.main()


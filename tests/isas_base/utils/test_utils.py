import unittest

import numpy as np

from isas_base.utils import get_cfg, set_list_dim


class TestUtils(unittest.TestCase):
    def test_set_list_dim(self):
        a = set_list_dim([1], 2)
        _a = np.asarray(a)
        self.assertEqual(_a.ndim, 2)
        self.assertEqual(a[0][0], 1)

    def test_get_cfg_remaining_none(self):
        default_cfg = {
            'a': None,
            'b': 1,
            }
        with self.assertRaises(ValueError):
            get_cfg(None, default_cfg)

    def test_get_cfg_overwrite(self):
        default_cfg = {
            'a': None,
            'b': 1,
            }
        cfg = get_cfg({'a': 0}, default_cfg)
        self.assertIsInstance(cfg, dict)
        self.assertEqual(set(cfg.keys()), set(default_cfg.keys()))
        self.assertEqual(cfg['a'], 0)
        self.assertEqual(cfg['b'], 1)


if __name__ == '__main__':
    unittest.main()

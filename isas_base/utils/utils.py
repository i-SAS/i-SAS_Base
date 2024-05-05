from copy import deepcopy
from typing import Any

import numpy as np


def set_list_dim(list_, dim):
    """set dimension of list

    Args:
        list_ (list): list.
        dim (int): dimension to be set

    Returns:
        list: list with the dimension

    Example:
        >>> set_list_dim([1], 2)
        [[1]]
    """
    if list_ is None:
        return list_
    _l = np.asarray(list_)
    diff_dim = dim - _l.ndim
    assert diff_dim >= 0, 'Invalid dimension of input.'
    if diff_dim == 0:
        return list_
    shape = [1]*diff_dim + list(_l.shape)
    _l = _l.reshape(shape)
    return _l.tolist()


def get_cfg(
        params: dict[str, Any] | None,
        default_cfg: dict[str, Any],
        ) -> dict[str, Any]:
    """Set config.

    Args:
        params: Parameters to overwrite config.
        default_cfg: A default config. 'None' values must be overwritten.

    Returns:
        A config updated.
    """
    cfg = deepcopy(default_cfg)
    if params is not None:
        _update_cfg(cfg, params)
    error_keys = _check_cfg_none(cfg, None, [])
    if len(error_keys) > 0:
        raise ValueError(f'Following keys remain None: {error_keys}')
    return cfg


def _update_cfg(
        cfg: dict[str, Any],
        params: dict[str, Any],
        ) -> None:
    """Update nested config.

    Args:
        cfg: A config to be updated.
        params: Parameters to overwrite config.
    """
    for k, v in params.items():
        if isinstance(v, dict) and k in cfg:
            _update_cfg(cfg[k], v)
        else:
            cfg[k] = v


def _check_cfg_none(
        cfg: dict[str, Any],
        parent_key: str,
        error_keys: list[str],
        ) -> list[str]:
    """Check if None value does not remain in nested config.

    Args:
        cfg: A config to be checked.
        parent_key: The name of parent key.
        error_keys: A list of keys with None value.

    Returns:
        A list of keys with None value.
    """
    for k, v in cfg.items():
        _parent_key = k if parent_key is None else f'{parent_key}.{k}'
        if isinstance(v, dict):
            error_keys = _check_cfg_none(v, _parent_key, error_keys)
        else:
            if v is None:
                error_keys.append(_parent_key)
    return error_keys

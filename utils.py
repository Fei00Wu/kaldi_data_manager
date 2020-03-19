import os
import numpy as np
from typing import *

T = TypeVar('T')


def chain_fold(A: Iterable[T],
               λ: Callable[[T, T], T],
               init: T) -> Iterable[T]:
    if A:
        step = λ(A[0], init)
        yield step
        yield from chain_fold(A[1:], λ, step)


def remove_empty(data_dir: str) -> None:
    """
    Removes empty files in given directory
    :param data_dir: full path to directory
    :return: None
    """
    for f in os.listdir(data_dir):
        full_path = os.path.join(data_dir, f)
        if os.path.isfile(full_path) and os.stat(full_path).st_size == 0:
            os.remove(full_path)


def random_assignment(*, ratio: List[float],
                      num_id: int) -> Iterable[int]:

    num_splits = len(ratio)
    np_ratio = np.array(ratio)
    np_ratio /= np.sum(np_ratio)
    accumulate_rate = np.array(list(chain_fold(ratio, lambda x, y: x + y, 0))).reshape(-1, 1)

    rand_res = np.random.rand(num_id)
    rand_res = rand_res.reshape([1, -1])

    acc_rate = np.repeat(accumulate_rate, num_id, 1)  # [num_splits, num_id]
    rand_res = np.repeat(rand_res, num_splits, 0)  # [num_splits, num_id]
    mask = np.where(rand_res <= acc_rate, 1.0, 0.0)  # [num_splits, num_id]

    category = np.arange(num_splits, 0, -1).reshape(-1, 1)
    category = np.repeat(category, num_id, 1)

    assignments = np.argmax(mask * category, 0)  # [num_id]
    assignments = assignments.tolist()

    return assignments
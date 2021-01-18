import time
import random
import contextlib


@contextlib.contextmanager
def timer():
    """ Time the execution of a context block.

    Yields:
        None
    """
    time_start = time.time()

    yield

    elapsed = time.time() - time_start
    print(f'Time: {elapsed:.2f}s')


def serialize(variable):
    """ Serialize object to str.

    Args:
        variable (dict | list | int | str): Item to serialize.

    Returns:
        str

    """

    # If dict, sort keys for reproducibility, then join keys and values by '_'.
    if type(variable) == dict:
        return '_'.join([f'{k}_{v}' for k, v in sorted(variable.items())])

    # If iterable, join items by '_'.
    elif type(variable) in (list, tuple):
        return '_'.join(map(str, variable))

    # Otherwise simply convert to string.
    return str(variable)


def sample_dict(dict_, n_samples, random_seed=None):
    """ Sample keys from dictionary.

    Args:
        dict_ (dict): Dict to sample, with each value being a list.
        n_samples (int): Number of items to sample.
        random_seed (int): Random seed.

    Returns:
        [dict, ..]

    Examples:
        >>> dict_to_sample = {'a': [1, 2], 'b': [3, 4, 5]}
        >>> print(sample_dict(dict_to_sample), 2)
        [{'a': 2, 'b': 5}, {'a': 1, 'b': 3}]

    """

    if random_seed is not None:
        random.seed(random_seed)

    random_samples = []
    for _ in range(n_samples):
        sample = {}
        while len(sample) == 0 or sample in random_samples:
            for key, values in sorted(dict_.items()):  # sort to ensure random consistency
                sample[key] = random.choice(values)
        random_samples.append(sample)

    return random_samples

import time
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


def serialize_dict(dict_):
    """ Serialize dict to str, sorting keys.

    Args:
        dict_: Dict to sort.

    Returns:
        str

    """

    return '_'.join([f'{k}_{v}' for k, v in sorted(dict_.items())])

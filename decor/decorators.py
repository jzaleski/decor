__all__ = (
    'class_property',
    'lazy_property',
    'memoized',
    'retryable',
    'timed',
)


import time

from functools import wraps


def class_property(receiver):
    """Class property decorator (exposes the decorated function as a `property`
    bound to the instance's `type`/`class`)

    Args:
        receiver (function): a function to wrap

    Returns:
        WrappedProperty: the wrapped and re-scoped function
    """
    class WrappedProperty(object):
        def __init__(self):
            self.method_name = receiver.__name__

        def __delete__(self, obj):
            raise AttributeError('Cannot delete "%s"' % self.method_name)

        def __get__(
            self,
            obj,
            cls=None
        ):
            return receiver(cls or type(obj))

        def __set__(
            self,
            obj,
            val
        ):
            raise AttributeError('Cannot set "%s"' % self.method_name)
    return WrappedProperty()


def lazy_property(receiver):
    """Lazy property decorator (caches the result of the decorated function
    after the first invocation, but quacks like a `property`)

    Args:
        receiver (function): a function to wrap

    Returns:
        property: the wrapped and re-scoped property
    """
    key = '_%s' % receiver.__name__

    @wraps(receiver)
    def with_memoization(self):
        if not hasattr(self, key):
            setattr(
                self,
                key,
                receiver(self)
            )
        return getattr(self, key)
    return property(with_memoization)


def memoized(receiver):
    """Memoization decorator (caches the result of the decorated function after
    the first invocation)

    This decorator will provide a constant time execution for methods called
    with the same inputs multiple times. It also allows the implementor to
    write more concise code w/o the need to worry about caching in multiple
    places.

    ---

    When generating the `key` we will take the `str` value of `self`, the
    `receiver#name` and the `str` value of each `arg` in `args` (the `str`
    value `of `arg.__dict__` when `arg` is an `object`).

    Args:
        receiver (function): a function to wrap

    Returns:
        function: the decorated function
    """
    def generate_arg_key(arg):
        return str(arg if not hasattr(arg, '__dict__') else arg.__dict__)

    def generate_args_key(*args):
        return ''.join(generate_arg_key(arg) for arg in args)

    def generate_key(receiver, *args):
        return ''.join([generate_receiver_key(receiver),
            generate_args_key(*args)])

    def generate_receiver_key(receiver):
        return receiver.__name__

    @wraps(receiver)
    def with_memoization(self, *args):
        if not hasattr(self, '_cache'):
            self._cache = {}
        cache = self._cache
        key = generate_key(receiver, *args)
        if key not in cache:
            cache[key] = receiver(self, *args)
        return cache[key]
    return with_memoization


def retryable(
    handled_exception_types=None,
    max_retries=None,
    sleep_times=None,
    on_retry_callback=None,
    on_success_callback=None,
    on_failure_callback=None,
    should_retry_callback=None,
    sleep_time_callback=None
):
    """Retry[able] decorator (retries the decorated function based on the
    provided configuration)

    Args:
        handled_exception_types (iterable of type): a iterable of `Exception`
            type(s)
            (default: `(Exception,)` e.g. everything)
        max_retries (int): the desired maximum number of retries
            (default: `3`)
        sleep_times (list of int|float): a list of sleep times (in seconds). In
            the case of a "retry" the current `attempt[_number]` will be used
            to index into this list
            (default: `[0, 1, 2]`)
        on_retry_callback (function): a function to call if/when a retry
            occurs
            (default: `None`)
        on_success_callback (function): a function to call if/when the wrapped
            function is successfully executed
            (default: `None`)
        on_failure_callback (function): a function to call if/when the wrapped
            function fails fatally
            (default: `None`)
        should_retry_callback (function): a function to call in order to
            determine if the wrapped function should be retried based on the
            current attempt number and exception raised
            (default: `attempt < max_attempts`)
        sleep_time_callback (function): a function to call in order to
            determine the sleep time, between attempts, based on the current
            attempt number and exception raised
            (default: `sleep_times[retry_attempt]`)

    Returns:
        function: the decorated function
    """
    if not handled_exception_types:
        handled_exception_types = (Exception,)

    if max_retries is None:
        max_retries = 3

    max_attempts = max_retries + 1

    if not sleep_times:
        sleep_times = [
            0,
            1,
            2,
        ]

    if not on_retry_callback:
        def on_retry_callback(
            self,
            retry_attempt,
            exception
        ):
            return None

    if not on_success_callback:
        def on_success_callback(self, total_attempts):
            return None

    if not on_failure_callback:
        def on_failure_callback(
            self,
            total_attempts,
            exception
        ):
            return None

    if not should_retry_callback:
        def should_retry_callback(
            self,
            attempt,
            exception
        ):
            return attempt < max_attempts

    if not sleep_time_callback:
        def sleep_time_callback(
            self,
            retry_attempt,
            exception
        ):
            return sleep_times[retry_attempt]

    def decorator(receiver):
        @wraps(receiver)
        def with_retry(
            self,
            *args,
            **kwargs
        ):
            attempt = 0
            last_exception = None
            while attempt < max_attempts:
                try:
                    attempt += 1
                    result = receiver(
                        self,
                        *args,
                        **kwargs
                    )
                    on_success_callback(self, attempt)
                    return result
                except tuple(handled_exception_types) as e:
                    last_exception = e
                    if not should_retry_callback(
                        self,
                        attempt,
                        e
                    ):
                        break
                    on_retry_callback(
                        self,
                        attempt,
                        e
                    )
                    sleep_time = sleep_time_callback(
                        self,
                        attempt - 1,
                        e
                    )
                    time.sleep(sleep_time)
            on_failure_callback(
                self,
                attempt,
                last_exception
            )
            raise last_exception
        return with_retry
    return decorator


def timed(
    stats_client=None,
    prefix=None,
    sample_rate=None
):
    """Timing decorator (records the time spent in the decorated method)

    Args:
        stats_client (object): a statistics client (the object must have a
            `record_timing` method that takes in 3 positional arguments:
            `stat_name`, `total_time` and `sample_rate`)
        prefix (str): a statistics prefix (default: `''`)
        sample_rate (int|float): a sample rate (default: `1.0` or 100%)

    Returns:
        function: the decorated method
    """
    assert(stats_client and hasattr(stats_client, 'record_timing'))

    prefix = str(prefix) + '.' if prefix is not None else ''
    sample_rate = sample_rate if sample_rate is not None else 1.0

    def decorator(receiver):
        @wraps(receiver)
        def with_timing(
            self,
            *args,
            **kwargs
        ):
            stat_name = '{}{}'.format(prefix, receiver.__name__)
            start_time = time.time()
            result = receiver(
                self,
                *args,
                **kwargs
            )
            total_time = (time.time() - start_time) * 1000.0
            stats_client.record_timing(
                stat_name,
                total_time,
                sample_rate
            )
            return result
        return with_timing
    return decorator

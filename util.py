import time


# A helper method that waits until a certain condition is met
def wait_until(somepredicate, timeout, period, *args, **kwargs):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if somepredicate(*args, **kwargs): return True
        time.sleep(period)
    return False

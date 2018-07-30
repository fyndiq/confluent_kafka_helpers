from collections import deque, defaultdict


def roundrobin(*iterables):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    iterators = deque(map(iter, iterables))
    while iterators:
        try:
            while True:
                yield next(iterators[0])
                iterators.rotate(-1)
        except StopIteration:
            # Remove an exhausted iterator.
            iterators.popleft()


log = defaultdict(lambda: defaultdict(deque))
log['foo'][0] = ('foo', 0, deque(['a', 'b']))
log['foo'][1] = ('foo', 1, deque(['c', 'd']))
log['foo'][2] = ('foo', 2, deque(['e', 'f']))
# log['bar'][3] = ('bar', 0, deque(['g', 'h']))
# log['bar'][4] = ('bar', 1, deque(['i', 'j']))
# log['bar'][5] = ('bar', 2, deque(['k', 'l']))

# log['foo'][0] = ('foo', 0, deque())
# log['foo'][1] = ('foo', 1, deque())
# log['foo'][2] = ('foo', 2, deque())

flat = [list(topic[1].values()) for topic in log.items()]


def poll():
    for topic, partition, value in roundrobin(*flat):
        while value:
            print(value.popleft())
            return True
        else:
            print(f"EOF {partition}")
            return False


eof = []
while True:
    message = poll()
    if not message:
        eof.append(True)
        if len(eof) == 3:
            break

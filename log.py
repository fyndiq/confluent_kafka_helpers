from collections import deque, defaultdict
log = defaultdict(lambda: defaultdict(deque))
log['foo'][0].append('foo')
print(log)
topics = ['foo', 'bar']
# for topic in topics:
#     for i in range(8):
#         log[topic][i]

[log[topic][i] for topic in topics for i in range(8)]

print(log)

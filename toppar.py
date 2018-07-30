from collections import deque, defaultdict


log = defaultdict(lambda: defaultdict(deque))
log['foo'][0] = deque(['a', 'b'])
log['foo'][1] = deque(['c', 'd'])
log['foo'][2] = deque(['e', 'f'])
log['bar'][3] = deque(['g', 'h'])
log['bar'][4] = deque(['i', 'j'])
log['bar'][5] = deque(['k', 'l'])


consuming_topics_partitions = {'foo': None, 'bar': 4}

filtered_log = defaultdict(dict)
for topic, partition in consuming_topics_partitions.items():
    if partition:
        filtered_log[topic][partition] = log[topic][partition]
    else:
        filtered_log[topic] = log[topic]

print(filtered_log)
messages = [[{'foo': {'0': ['a', 'b']}}, {'foo': {'1': ['c', 'd']}}]

# topic_messages = []
# for topic, partition in consuming_topics_partitions.items():
#     if partition:
#         topic_messages.append([log[topic][partition]])
#     else:
#         topic_messages.append([messages for messages in log[topic].values()])
# print(topic_messages)

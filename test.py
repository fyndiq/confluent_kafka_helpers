log = {
    'foo': {
        0: [1, 2, 3],
        1: [4, 5, 6]
    },
    'bar': {
        0: [7, 8, 9]
    },
}
# topics = {
#     t_k: p_k
#     for t_k, t_v in log.items()
#     for p_k, p_v in t_v.items()
# }
topics = {
    topic: [key for key, value in partitions.items()]
    for topic, partitions in log.items()
}

print(topics)

# p_v, p_k = log['foo'].items()
# print(p_v, p_k)

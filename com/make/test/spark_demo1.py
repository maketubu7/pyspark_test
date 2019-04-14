from pyspark import SparkContext

sc = SparkContext('local')
doc = sc.parallelize([['a', 'b', 'c'], ['b', 'd', 'd']])
words = doc.flatMap(lambda d: d).distinct().collect()
word_dict = {w: i for w, i in zip(words, range(len(words)))}
word_dict_b = sc.broadcast(word_dict)


def wordCountPerDoc(d):
    dict_new = {}
    wd = word_dict_b.value
    for w in d:
        if wd[w] in dict_new:
            dict_new[wd[w]] += 1
        else:
            dict_new[wd[w]] = 1
    return dict_new


print(doc.map(wordCountPerDoc).collect())
print("successful!")
# -*- coding: utf-8 -*-
# @Time    : 2019/4/12 15:33
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : mlib_test2.py
# @Software: PyCharm
from pyspark import SparkContext
import matplotlib.pyplot as plt
from matplotlib.pyplot import hist

sc = SparkContext(appName="PythonLR",master="local[*]")

user_data = sc.textFile('E:\pyspark_test\data\ml-100k\\u.user')
user_fields = user_data.map(lambda line: line.split('|'))
num_users = user_fields.map(lambda fields: fields[0]).count()   #统计用户数
num_genders = user_fields.map(lambda fields : fields[2]).distinct().count()   #统计性别个数
num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()   #统计职业个数
num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()   #统计邮编个数
print ("Users: %d, genders: %d, occupations: %d, ZIP codes: %d"%(num_users,num_genders,num_occupations,num_zipcodes))


ages = user_fields.map(lambda x: int(x[1])).collect()
hist(ages, bins=20, color='lightblue',normed=True)
fig = plt.gcf()
fig.set_size_inches(12,6)
plt.show()

#画出用户的职业的分布图：

import numpy as np
count_by_occupation = user_fields.map(lambda fields: (fields[3],1)).reduceByKey(lambda x,y:x+y).collect()
print(count_by_occupation)
x_axis1 = np.array([c[0] for c in count_by_occupation])
y_axis1 = np.array([c[1] for c in count_by_occupation])
x_axis = x_axis1[np.argsort(y_axis1)]
y_axis = y_axis1[np.argsort(y_axis1)]
pos = np.arange(len(x_axis))
width = 1.0
ax = plt.axes()
ax.set_xticks(pos+(width)/2)
ax.set_xticklabels(x_axis)

plt.bar(pos, y_axis, width, color='lightblue')
plt.xticks(rotation=30)
fig = plt.gcf()
fig.set_size_inches(12,6)
plt.show()

movie_data = sc.textFile("E:\pyspark_test\data\ml-100k\\u.item")
print(movie_data.first())
num_movies = movie_data.count()
print ('Movies: %d' % num_movies)

#画出电影的age分布图：
import matplotlib.pyplot as plt
from matplotlib.pyplot import hist
import numpy as np
def convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900

movie_fields = movie_data.map(lambda lines:lines.split('|'))
years = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x))
years_filtered = years.filter(lambda x: x!=1900)
print(years_filtered.count())
movie_ages = years_filtered.map(lambda yr:1998-yr).countByValue()

print(movie_ages)

values = movie_ages.values()
bins = movie_ages.keys()
re_values = []
for bin in bins:
    for i in range(movie_ages[bin]):
        re_values.append(bin)
# print(re_values)
print('----------------------')
hist(re_values, bins=72,color='lightblue',normed=True)
fig = plt.gcf()
fig.set_size_inches(12,6)
plt.show()

rating_data = sc.textFile('E:\pyspark_test\data\ml-100k\\u.data')
print(rating_data.first())
num_ratings = rating_data.count()
print('Ratings: %d'% num_ratings)

rating_data = rating_data.map(lambda line: line.split('\t'))
ratings = rating_data.map(lambda fields: int(fields[2]))
max_rating = ratings.reduce(lambda x,y:max(x,y))
min_rating = ratings.reduce(lambda x,y:min(x,y))
mean_rating = ratings.reduce(lambda x,y:x+y)/num_ratings
median_rating = np.median(ratings.collect())
ratings_per_user = num_ratings/num_users    #每个人的评分数量
ratings_per_movie = num_ratings/ num_movies  #每部电影的评分数量
print('Min rating: %d' %min_rating)
print('max rating: %d' % max_rating)
print('Average rating: %2.2f' %mean_rating)
print('Median rating: %d '%median_rating)
print('Average # of ratings per user: %2.2f'%ratings_per_user)
print('Average # of ratings per movie: %2.2f' % ratings_per_movie)
print(ratings.stats())  #dataframe.describe()

count_by_rating = ratings.countByValue()
x_axis = np.array([str(x) for x in count_by_rating.keys()])
y_axis = np.array([float(c) for c in count_by_rating.values()])
y_axis_normed = y_axis/y_axis.sum()
pos = np.arange(len(x_axis))
width = 1.0
ax = plt.axes()
ax.set_xticks(pos+(width/2))
ax.set_xticklabels(x_axis)

plt.bar(pos, y_axis_normed, width, color='lightblue')
plt.xticks(rotation=30)
fig = plt.gcf()
fig.set_size_inches(12,6)
plt.show()


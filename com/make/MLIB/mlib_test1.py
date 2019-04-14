# -*- coding: utf-8 -*-
# @Time    : 2019/4/12 14:38
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : mlib_test1.py
# @Software: PyCharm

from pyspark import SparkContext


sc = SparkContext(appName="PythonLR",master="local[*]")
user_data = sc.textFile('E:\pyspark_test\data\ml-100k\\u.user')
user_fields = user_data.map(lambda line: line.split('|'))
num_users = user_fields.map(lambda fields: fields[0]).count()   #统计用户数
num_genders = user_fields.map(lambda fields : fields[2]).distinct().count()   #统计性别个数
num_occupations = user_fields.map(lambda fields: fields[3]).distinct().count()   #统计职业个数
num_zipcodes = user_fields.map(lambda fields: fields[4]).distinct().count()   #统计邮编个数
print ("Users: %d, genders: %d, occupations: %d, ZIP codes: %d"%(num_users,num_genders,num_occupations,num_zipcodes))

import matplotlib.pyplot as plt
# from matplotlib.pyplot import hist
# ages = user_fields.map(lambda x: int(x[1])).collect()
# hist(ages, bins=20, color='lightblue',normed=True)
# fig = plt.gcf()
# fig.set_size_inches(12,6)
# plt.show()

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


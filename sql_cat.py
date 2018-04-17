import sys
from pyspark import SparkContext, SparkConf
import cv2
import os
import re
import binascii

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

# sys.setdefaultencoding('utf8')
# reload(sys)

conf = SparkConf().setAppName("cat").setMaster("yarn")
sc = SparkContext(conf=conf)

# find_path = '/var/www/html/Spark_SQL'
count = 1
find_path = '/var/www/html/database'

class HbaseWrite():
    def __init__(self):
        # self.tableName = 'cat_test'
        self.tableName = 'cat'
        self.transport = TSocket.TSocket('student62', 9090)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.transport.open()
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)

    def createTable(self):
        col_list = []
        # for i in range(1,4): 
        for i in range(1,1001):
            col_list.append(ColumnDescriptor(name="CF%s:" % i, maxVersions=1))
        # col2 = ColumnDescriptor(name="feature:", maxVersions=1)
        self.client.createTable(self.tableName, col_list)

    def write(self, row, column_key, column_value):
        self.client.mutateRow(self.tableName, row, [Mutation(column=column_key, value=column_value)])

    def read(self, PicName):
        row = PicName.split('.')[0]
        data_type = PicName.split('.')[1]
        get_data = self.client.get(self.tableName, row, 'data:%s' % data_type, {})[0]
        if get_data:
            return get_data.value
        else:
            return 'Error'


def find_img(_path):   
    find_file = re.compile(r'^[0-9a-zA-Z\_]*.jpg$')
    find_walk = os.walk(_path)
    img=[]

    for path, dirs, files in find_walk:
        for f in files:
            if find_file.search(f):
                path_name = path
                file_name = f
                img_withname = []
                img_withname.append(f)
                img_withname.append(cv2.imread(path_name + '/' + file_name))
                img.append(img_withname)
                #                 WHB.write(path_name, file_name)  
    return img 

# def get_img_fromdb():
#     find_file = re.compile(r'^[0-9a-zA-Z\_]*.jpg$')
#     find_walk = os.walk(_path)
#     WHB = HbaseWrite()
#     for path, dirs, files in find_walk:
#         for f in files:
#             if find_file.search(f):
#                 # path_name = path
#                 file_name = f
#                 WHB.read(file_name)
                
     

# def getdiff(name,img):
#     Sidelength=30
#     img=cv2.resize(img,(Sidelength,Sidelength),interpolation=cv2.INTER_CUBIC)
#     gray=cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)

#     avglist=[]
#     for i in range(Sidelength):
#         avg=sum(gray[i])/len(gray[i])
#         avglist.append(avg)
#     WHB = HbaseWrite()
#     WHB.write_feature(str(avglist), name) 
#     return avglist

def getdiff(img):
    Sidelength=30
    img=cv2.resize(img,(Sidelength,Sidelength),interpolation=cv2.INTER_CUBIC)
    gray=cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)

    avglist=[]
    for i in range(Sidelength):
        avg=sum(gray[i])/len(gray[i])
        avglist.append(avg)
    return avglist

def getimage(path_name, file_name):
    return cv2.imread(path_name + '/' + file_name)



# print(====================start find image===============================)
# img = find_img(find_path)
# l = get_img_fromdb()
# print(l)

def main(_path, count):
    WHB = HbaseWrite()
    WHB.createTable()
    find_file = re.compile(r'^[0-9a-zA-Z\_]*.jpg$') 
    find_walk = os.walk(_path)
    for path, dirs, files in find_walk:
        for f in files:
            if find_file.search(f):
                path_name = path
                file_name = f
                rowT = count/1000 + 1
                # for num in range(1,4):
                for num in range(1,1001):
                    # WHB.write(str(1), 'CF%s:Name%s' % (num,num), file_name)
                    # WHB.write(str(1), 'CF%s:Feature%s' % (num,num), str(getdiff(getimage(path_name,file_name))))
                    WHB.write(str(rowT), 'CF%s:Name%s' % (num,num), file_name)
                    WHB.write(str(rowT), 'CF%s:Feature%s' % (num,num), (num,num), str(getdiff(getimage(path_name,file_name))))
                    count = count + 1

if __name__ == '__main__':
    main(find_path, count)
    # for j in range(0,10):
    #     rdd=[]
    #     for i in range(0,50):
#         rdd.append(img[i])

#     data = sc.parallelize(rdd,16)
#     data1 = data.map(lambda x:[x[0],getdiff(x[0],x[1])]).collect()
#     img = img[:0]+img[100:]
    




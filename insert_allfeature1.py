import sys
from cv2 import inRange

reload(sys)
from pyspark import SparkContext, SparkConf
import cv2
sys.setdefaultencoding('utf8')
import os
import re
import binascii

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

conf = SparkConf().setAppName("duyongzhi_i").setMaster("yarn")
sc = SparkContext(conf=conf)

#find_path = '/var/www/html/Spark_SQL'
find_path = '/var/www/html/database'

class HbaseWrite():
    def __init__(self):
        self.tableName = 'database'
        self.transport = TSocket.TSocket('student62', 9090)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.transport.open()
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)

    def createTable(self):
        col1 = ColumnDescriptor(name="data:", maxVersions=1)
        # col2 = ColumnDescriptor(name="feature:", maxVersions=1)
        self.client.createTable(self.tableName, [col1])

    def write(self, PicPath, PicName):
        row = PicName.split('.')[0]
        _data = PicName.split('.')[1]
        PicData = open('%s/%s' % (PicPath, PicName), 'rb').read()
        PicData = binascii.hexlify(PicData)
        # TypeError: mutateRow() takes exactly 5 arguments (4 given)
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:%s' % _data, value=PicData)])
    
    def write_feature(self, PicFeature, PicName):
        row = PicName.split('.')[0]
        _data = PicName.split('.')[1]
        # PicData = open('%s/%s' % (PicPath, PicName), 'rb').read()
        # PicData = binascii.hexlify(PicData)
        # TypeError: mutateRow() takes exactly 5 arguments (4 given)
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:feature', value=PicFeature)])

    def read(self, tableName, PicName):
        row = PicName.split('.')[0]
        data_type = PicName.split('.')[1]
        get_data = self.client.get(tableName, row, 'data:%s' % data_type, {})[0]
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

def getdiff(name,img):
    Sidelength=30
    img=cv2.resize(img,(Sidelength,Sidelength),interpolation=cv2.INTER_CUBIC)
    gray=cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)

    avglist=[]
    for i in range(Sidelength):
        avg=sum(gray[i])/len(gray[i])
        avglist.append(avg)
    WHB = HbaseWrite()
    WHB.write_feature(str(avglist), name)
    print "insert!"
    return avglist
    

img = find_img(find_path)
#print(data.glom().collect())
#print(img[0])
    
for j in range(0,200):
    rdd=[]
    for i in range(0,50):
        rdd.append(img[i])

        #print(rdd.collect())
    data = sc.parallelize(rdd,16)
    #print(data.glom().collect())
    data1 = data.map(lambda x:[x[0],getdiff(x[0],x[1])]).collect()
    img = img[:0]+img[100:]
#     for i in range(0,100):
#         img.pop(0)
    print("---------------------------------------------------")
#print(data1)
#WHB = HbaseWrite()
#for i in range(len(data1)):
#    print(data1[i][0])
#    print(data1[i][1])
#    WHB.write_feature('feature:'+str(data1[i][1]), data1[i][0])
print("===================================================")



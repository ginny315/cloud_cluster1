from hdfs import *
import io
import binascii
# from PIL import Image
import cv2
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
import ast

client = Client("http://student62:50070")
find_path = '/var/www/html/database'

def getss(list): 
    avg=sum(list)/len(list)    
    ss=0  
    for l in list:
        ss+=(l-avg)*(l-avg)/len(list)       
    return ss

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
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:feature', value=PicFeature)])

    def read(self, tableName, PicName):
        row = PicName.split('.')[0]
        data_type = PicName.split('.')[1]
        get_data = self.client.get(tableName, row, 'data:%s' % data_type)[0]
        if get_data:
            return get_data.value
        else:
            return 'Error'

    def read_feature(self, tableName, PicName):
        row = PicName.split('.')[0]
        data_type = PicName.split('.')[1]
        get_data = self.client.get(tableName, row, 'data:feature')[0]
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
    return img 


def detect_human_feature():
    cut_cat_path = 'static/image/face.jpeg'
    img = cv2.imread(cut_cat_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    human_haar_cascade_path = "static/haarcascade/haarcascade_frontalface_default.xml"
    face_cascade_human = cv2.CascadeClassifier(human_haar_cascade_path)
    faces = face_cascade_human.detectMultiScale(
        gray,
        scaleFactor=1.14,
        minNeighbors=5,
        minSize=(5, 5),
        flags=cv2.CASCADE_SCALE_IMAGE
    )
    for(x, y, w, h) in faces:
        crop_img = img[y:y+h, x:x+w]
        res = cv2.resize(crop_img, (128, 128), interpolation=cv2.INTER_CUBIC)
    return res

def get_diff(img):
    side_length = 30
    img = cv2.resize(img, (side_length, side_length), interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    avg_list = []
    for i in range(side_length):
        avg = sum(gray[i]) / len(gray[i])
        avg_list.append(avg)
    return avg_list

def getavg(f1,f2):
    difflist=[]
    for i in range(30):
        avg1=f1[i]-f2[i]
        difflist.append(avg1)
    return difflist

res = detect_human_feature()
diff_human = get_diff(res)

img = find_img(find_path)
WHB = HbaseWrite()
list1=[]
feature=[]

for i in range(0, 9949):
#for i in range(0, 50):
    temp=WHB.read_feature('database',img[i][0])
    if temp != 'Error':
        print "i = "+ str(i) + "    " +str(img[i][0])
        feature.append([img[i][0], ast.literal_eval(temp)])
        list1.append(feature[i])
    else:
        print "error"

conf = SparkConf().setAppName("similiarty").setMaster("yarn")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
data = sc.parallelize(list1,8)

data1 = data.map(lambda x:[x[0],x[1]])
data2 = data1.map(lambda x:[x[0],getavg(x[1],diff_human)])
data3 = data2.map(lambda x:[x[0],getss(x[1])])
data4 = data3.sortBy(lambda x: x[1], ascending=True).take(1)
print("---------------------------------------------------")
print(data4[0][0])
# coding:utf-8

import sys


reload(sys)

sys.setdefaultencoding('utf8')

#    __author__ = '�� �'

#    __date__ = '2016/9/5'

#    __Desc__ = �������С���ӣ���ԲȦȦ������

# ��ȡͼƬ

import cv2

faceCascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml') 
  
img = cv2.imread('renmao.jpg')
#img = cv2.imread('clw.png')  
#img = cv2.imread('clw.jpg')  
  
gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
  
faces = faceCascade.detectMultiScale(  
    gray,  
    scaleFactor= 1.14,  
    minNeighbors=5,  
    minSize=(5, 5),  
    flags = cv2.CASCADE_SCALE_IMAGE
)  
  
 

for(x,y,w,h) in faces:

    crop_img = img[y:y+h, x:x+w]
    res=cv2.resize(crop_img,(128,128),interpolation=cv2.INTER_CUBIC)
    cv2.imwrite('cut_human.jpg',res)
    cv2.rectangle(img,(x,y),(x+w,y+w),(0,255,0),2)
    
    
cv2.imshow("Find Faces!",img)

cv2.waitKey(0)
cv2.destroyAllWindows()
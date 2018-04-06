# coding:utf-8

import sys


reload(sys)

sys.setdefaultencoding('utf8')

#    __author__ = '郭 璞'

#    __date__ = '2016/9/5'

#    __Desc__ = 人脸检测小例子，以圆圈圈出人脸

# 读取图片

import cv2

faceCascade = cv2.CascadeClassifier('haarcascade_frontalcatface.xml') 
  
imgpath = 'renmao.jpg'
img = cv2.imread('renmao.jpg')    
  
gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
  
faces = faceCascade.detectMultiScale(  
    gray,  
    scaleFactor= 1.10,  
    minNeighbors=3,  
    minSize=(5, 5),  
    flags = cv2.CASCADE_SCALE_IMAGE
)  
  
 

for(x,y,w,h) in faces:

    cv2.rectangle(img,(x,y),(x+w,y+w),(0,255,0),2)
    crop_img = img[y:y+h, x:x+w]
    cv2.imwrite('lala1.jpg',crop_img)


#cv2.imshow("Find Faces!",img)
print image
cv2.imwrite("test11.jpg",image)

cv2.waitKey(0)
# coding:utf-8
import cv2

faceCascade = cv2.CascadeClassifier('classify/haarcascade_frontalcatface.xml') 
  
img = cv2.imread('resource/renmao.jpg')    
  
gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
  
faces = faceCascade.detectMultiScale(  
    gray,  
    scaleFactor= 1.10,  
    minNeighbors=3,  
    minSize=(5, 5),  
    flags = cv2.CASCADE_SCALE_IMAGE
)  

def cutcat():
    for(x,y,w,h) in faces:
        crop_img = img[y:y+h, x:x+w]
        res=cv2.resize(crop_img,(128,128),interpolation=cv2.INTER_CUBIC)
        cv2.imwrite('dist/cut_cat.jpg',res)
        cv2.rectangle(img,(x,y),(x+w,y+w),(0,255,0),2)
    
    
# cv2.imshow("Find Faces!",img)

# cv2.waitKey(0)
# cv2.destroyAllWindows()
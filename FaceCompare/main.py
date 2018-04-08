# coding:utf-8
import cv2
import matplotlib.pyplot as plt
import cut_cat as cat
import cut_human as human
# faceCascade = cv2.CascadeClassifier('haarcascade_frontalcatface.xml') 
  
# img = cv2.imread('renmao.jpg')    
  
# gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
  
# faces = faceCascade.detectMultiScale(  
#     gray,  
#     scaleFactor= 1.10,  
#     minNeighbors=3,  
#     minSize=(5, 5),  
#     flags = cv2.CASCADE_SCALE_IMAGE
# )  
  
cat.cutcat()
human.cuthuman()

# for(x,y,w,h) in faces:
#     crop_img = img[y:y+h, x:x+w]
#     cv2.imwrite('lala1.jpg',crop_img)
#     cv2.rectangle(img,(x,y),(x+w,y+w),(0,255,0),2)

img1 = cv2.imread('dist/cut_cat.jpg')
img2 = cv2.imread('dist/cut_human.jpg')

#计算方差
def getss(list):
    #计算平均值
    avg=sum(list)/len(list)
    #定义方差变量ss，初值为0
    ss=0
    #计算方差
    for l in list:
        ss+=(l-avg)*(l-avg)/len(list)   
    #返回方差
    return ss

#获取每行像素平均值  
def getdiff(img2):
    #定义边长
    Sidelength=30
    #缩放图像
    img2=cv2.resize(img2,(Sidelength,Sidelength),interpolation=cv2.INTER_CUBIC)
    #灰度处理
    gray=cv2.cvtColor(img2,cv2.COLOR_BGR2GRAY)
    #avglist列表保存每行像素平均值
    avglist=[]
    #计算每行均值，保存到avglist列表
    for i in range(Sidelength):
        avg=sum(gray[i])/len(gray[i])
        avglist.append(avg)
    #返回avglist平均值   
    return avglist

#读取测试图片
diff1=getdiff(img2)
print('img2:',getss(diff1))

#读取测试图片
diff11=getdiff(img1)
print('img1:',getss(diff11))

print('similarity',getss(diff1)-getss(diff11))

x=range(30)  

plt.figure("avg")  
plt.plot(x,diff1,marker="*",label="$walk01$") 
plt.plot(x,diff11,marker="*",label="$walk03$") 
plt.title("avg")
plt.legend()
plt.show()
#cv2.CASCADE_SCALE_IMAGE

cv2.waitKey(0)
cv2.destroyAllWindows()




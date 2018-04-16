from PIL import Image
import io
import binascii

data = open('/Users/dyz/Desktop/IMG_1214.jpg','r').read()

print(type(data))
r_data = binascii.hexlify(data)
r_data = binascii.unhexlify(r_data)

stream = io.BytesIO(r_data)

img = Image.open(stream)
img.save("/Users/dyz/Desktop/a_test.png")
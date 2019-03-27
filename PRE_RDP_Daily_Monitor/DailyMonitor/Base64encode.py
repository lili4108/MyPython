from Crypto.Cipher import AES


AES.new()

copyright = '777777_Zx'

def encode(str):
    #转成bytes string
    bytesString = str.encode(encoding="utf-8")
    print('1st print' , bytesString)

    #base64 编码
    encodeby = base64.b64encode(bytesString)
    encodestr = str(encodeby,encoding = 'utf-8')

    return encodestr


def decode(encodestr):
    # 解码
    decodeby = base64.b64decode(encodestr)
    decodestr = str(decodeby,encoding = 'utf-8')

    return decodestr

if __name__ == '__main__':
    s1 = encode(copyright)
    print(s1)
    print(type(s1))
    s2 = decode(s1)
    print(s2)
    print(type(s2))
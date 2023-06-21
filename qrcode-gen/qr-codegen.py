import qrcode
from PIL import Image

logo_link = "./asset/logoWhiteBG.png"
logo = Image.open(logo_link)

basewidth = 200

wpercent = basewidth / float(logo.size[0])
hsize = int((float(logo.size[1]) * float(wpercent)))
logo = logo.resize((basewidth, hsize), Image.ANTIALIAS)
QRC = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_H)
url = None  # Insert URL here
QRC.add_data(url)
QRC.make()
QRimg = QRC.make_image(back_color="white").convert("RGB")

pos = (
    (QRimg.size[0] - logo.size[0]) // 2,
    (QRimg.size[1] - logo.size[1]) // 2,
)
QRimg.paste(logo, pos)

QRimg.save("./output/googleFormWithLogo.png")

# http://blog.iconfinder.com/detecting-duplicate-images-using-python/
from PIL import Image
from hamming_distance import hamming_distance

def dhash(image, hash_size = 8):
    # Grayscale and shrink the image in one step.
    image = image.convert('L').resize(
        (hash_size + 1, hash_size),
        Image.ANTIALIAS,
    )

    pixels = list(image.getdata())

    # Compare adjacent pixels.
    difference = []
    for row in xrange(hash_size):
        for col in xrange(hash_size):
            pixel_left = image.getpixel((col, row))
            pixel_right = image.getpixel((col + 1, row))
            difference.append(pixel_left > pixel_right)

    # Convert the binary array to a hexadecimal string.
    decimal_value = 0
    hex_string = []
    for index, value in enumerate(difference):
        if value:
            decimal_value += 2**(index % 8)
        if (index % 8) == 7:
            hex_string.append(hex(decimal_value)[2:].rjust(2, '0'))
            decimal_value = 0

    return ''.join(hex_string)

if __name__ == "__main__":
    image1 = Image.open("../images/hans1.jpg")
    image2 = Image.open("../images/hans2.jpg")
    hash1 = dhash(image1, 4)
    hash2 = dhash(image2, 4)
    print hash1
    print hash2
    print hamming_distance(hash1, hash2)

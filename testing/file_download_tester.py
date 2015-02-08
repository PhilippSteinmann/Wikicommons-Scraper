import urllib
for i in range(10):
    #urllib.urlretrieve("http://upload.wikimedia.org/wikipedia/commons/7/70/Nicoletto_Semitecolo_-_Two_Christians_before_the_Judges_-_WGA21152.jpg", "painting.jpg")
    urllib.urlretrieve("http://upload.wikimedia.org/wikipedia/commons/7/70/Nicoletto_Semitecolo_-_Two_Christians_before_the_Judges_-_WGA21152.jpg")

# Takes 2.8 seconds
# Conclusion: Image download definitely needs to be asynchronous

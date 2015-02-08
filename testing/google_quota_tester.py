import urllib2, time
num_requests = 0
while True:
    num_requests += 1
    response = urllib2.urlopen('https://www.google.com/webhp?sourceid=chrome-instant&ion=1&espv=2&es_th=1&ie=UTF-8#q=Reinier+Nooms+site:http:%2F%2Fcommons.wikimedia.org%2Fwiki%2F+filetype:png')
    html = response.read()
    print "Request count = %d" % (num_requests)
    print html[0:100]
    print
    time.sleep(0.01)

# Conclusion:
# Not feasible, because search results are obfuscated in HTML

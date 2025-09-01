import csv 


cdata = []
with open('companies.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    for row in spamreader:
        cdata.append(row)


for company, url in cdata:
    print (url)



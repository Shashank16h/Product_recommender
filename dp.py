import csv
import json
import gzip


'''

csv.register_dialect('myDialect',delimiter='\t',quoting=csv.QUOTE_ALL,skipinitialspace=True)
with open("amazon_reviews_us_Mobile_Electronics_v1_00.tsv","r",encoding="utf8") as f:
    reader = csv.DictReader(f,dialect='myDialect')
    c=0
    cid=[]
    for row in reader:
        #

'''

def parse(path):
  g = gzip.open(path, 'rb')
  for l in g:
    yield json.loads(l)

c=0
asin=[]
for d in parse("AMAZON_FASHION.json.gz"):
    asin.append(d['asin'])
    c+=1
    if c>2:
        break



with open("products.txt",'w') as f:
    k=['asin','title','brand','image','also_buy','price']
    for i in k:
        f.write(i+'\t')
    f.write('\n')
    for d in parse('meta_AMAZON_FASHION.json.gz'):
        if(d['asin'] in sa):
            for i in k:
                if(i in d):
                    if(i=='image'):
                        f.write(str(d[i][0])+'\t')
                    else:
                        f.write(str(d[i])+'\t')
                else:
                    f.write('NA'+'\t')
            f.write('\n')
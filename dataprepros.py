import json
import gzip
#from nltk.tokenize import word_tokenize

ns=0;nn=0;nr=0;nd=0;np=0
keys=[];rvid=[]
ut=1230768000 #2009
minSalRank=100000

def parse(path):
    g = gzip.open(path,'r')
    for l in g:
        yield json.loads(json.dumps(eval(l)))

asin=[]
for d in parse('./rawdataset/reviews_Electronics_5.json.gz'):
    asin.append(d['asin'])

asin.sort()
i=0;nn=len(asin);da=[]
while(i<nn):
    j=i
    while(j<nn and asin[i]==asin[j]):
        j+=1
    da.append((asin[i],j-i))
    i=j
sa=[i[0] for i in da if i[1]>=50]
nn=0

def extractProduct(path,asin,pname,pcat,pimg,salRank,rel):
    global minSalRank,sa
    g = gzip.open("./rawdataset/meta_"+path[21:-10]+".json.gz",'r')
    for l in g:
        d=json.loads(json.dumps(eval(l)))
        if(d['asin'] not in sa):
            continue
        if(not (('title' in d) and ('salesRank' in d and len(d['salesRank'])>=1))):
            continue
        if(d['salesRank'][next(iter(d['salesRank']))]>minSalRank):
            continue
        salRank.append(d['salesRank'][next(iter(d['salesRank']))])
        asin.append(d['asin'])
        pname.append(d['title'].replace('\t'," "))
        pcat.append(d['categories'][0][-1])
        if('imUrl' not in d):
            pimg.append("null")
        else:
            pimg.append("null" if d['imUrl']=="" else d['imUrl'])
        if('related' not in d):
            rel.append("null")
        else:
            ab=[]
            for i in d['related'].keys():
                if(i!='also_viewed'):
                    ab+=d['related'][i]
            rel.append(str(ab))
    tp=len(pname)
    #print(tp)

def extractNprocessReviewData(path):
    global ns,nn,nr,nd,np,keys,ut,rvid
    asin=[];pname=[];pcat=[];pimg=[];salRank=[];rel=[]
    extractProduct(path,asin,pname,pcat,pimg,salRank,rel)
    with open('processedData.txt','a') as fd, open('products.txt','a') as fp:
        id=0;casin="";op=1;prd=""
        for d in parse(path):
            if(casin!=d['asin']):
                if(op!=1 and prd!=""):
                    fd.write(prd)
                    fp.write(casin+'\t'+pname[id]+'\t'+path[21:-10]+'\t'+pcat[id]+'\t'+pimg[id]+'\t'+rel[id]+'\t'+str(salRank[id])+'\n')
                    np+=1;nr+=prd.count('\n')
                op=0;prd=""
                try:
                    id=asin.index(d['asin'])
                    casin=asin[id]
                except:
                    continue
            if(op==1):
                continue
            if(d['unixReviewTime']<ut):
                op=1
                continue
            #if((d['asin'],d['reviewerID']) in rvid):
            #    op=1;nd+=1
            #    continue
            tem=""
            for k in keys:
                if(k!='helpful' and k!='reviewTime' and k!='reviewText' and k!='summary'):
                    if(k=='reviewerName'):
                        tem+=('null') if (k not in d) or d[k]=='' else d[k]
                    else:
                        tem+=(str(d[k]))
                    tem+="\t"
            if(tem[0]=="\t" or ("\t\t" in tem)):
                nn+=1
                continue
            tem+="\n"
            prd+=tem
            #rvid.append((d['asin'],d['reviewerID']))
        if(op!=1 and prd!=""):
            fd.write(prd)
            fp.write(casin+'\t'+pname[id]+'\t'+path[21:-10]+'\t'+pcat[id]+'\t'+pimg[id]+'\t'+rel[id]+'\t'+str(salRank[id])+'\n')
            print(rel[id])
            np+=1;nr+=prd.count('\n')

for d in parse("./rawdataset/reviews_Electronics_5.json.gz"):
    keys=d.keys()
    break
with open('processedData.txt','a') as fd, open('products.txt','a') as fp:
    fd.write('reviewerID\tasin\treviewerName\tratings\treviewTime\n')
    fp.write('asin\ttitle\tcat\tsubCat\timgUrl\trelated\tsalRank\n')
print('Extracting produts & its reviews...')
extractNprocessReviewData("./rawdataset/reviews_Electronics_5.json.gz")
print("\nNo of products for which reviews are extracted: "+str(np)+"\nNo of reviews extracted: "+str(nr+nn+nd)+"\nNo of duplicates removed: "+str(nd))



'''
def extractNprocessReviewData(path,nr):
    global ns,nn,keys,ut
    stop_words = ['the','a','an','is']
    asin=[];pname=[];pcat=[];pimg=[];salRank=[];desc=[]
    extractProduct(path,asin,pname,pcat,pimg,salRank,desc)
    #with open('processedData.txt','a') as fd, open('products.txt','a') as fp:
    id=0;casin=""
    for d in parse(path,nr):
        if(d['unixReviewTime']<ut):
            continue
        if(casin!=d['asin']):
            try:
                id=asin.index(d['asin'])
            except:
                continue
        tem=""
        for k in keys:
            if(k!='helpful' and k!='reviewerName'):
                if(k=='reviewText' or k=='summary'):
                    wordTokens = word_tokenize(str(d[k]))
                    for w in wordTokens:
                        if w=="n't":
                            w="not"
                        if not w in stop_words:
                            tem+=w+" "
                        else:
                            ns+=1
                else:
                    tem+=(str(d[k]))
                tem+="\t"
        if(tem[0]=="\t" or ("\t\t" in tem)):
            continue
        tem+=pname[id]+'\t'+path[21:-10]+'\t'+pcat[id]+"\n"
        if(casin!=asin[id]):
            casin=asin[id]
            #fp.write(casin+'\t'+pname[id]+'\t'+path[21:-10]+'\t'+pcat[id]+'\t'+pimg[id]+'\t'+desc[id]+'\n')
        #fd.write(tem)
        nn+=1
'''
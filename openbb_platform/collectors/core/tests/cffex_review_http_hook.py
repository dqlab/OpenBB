import urllib.request, urllib.error, os, json
from urllib.parse import urlparse,parse_qs
class R:
 def __init__(self,b,s=200): self.b=b; self.status=s
 def __enter__(self): return self
 def __exit__(self,*a): pass
 def read(self,n): return self.b[:n]
def fake(req,timeout):
 u=req.full_url
 with open('/tmp/cffex-review-test/calls','a') as f: f.write(u+'\n')
 mode=os.environ.get('MODE','ok')
 if mode=='http503': raise urllib.error.HTTPError(u, 503, 'unavailable', {}, __import__('io').BytesIO(b'failure'))
 if mode=='networkfail': raise urllib.error.URLError('offline')
 if mode=='large': return R(b'x'*1000001,503)
 if mode=='larger': return R(b'x'*1000002,503)
 if mode=='empty': return R(b'')
 if mode=='204': return R(b'',204)
 if 'getOptionData' in u:
  t=parse_qs(urlparse(u).query)['pinzhong'][0]; p=t[:2].upper(); info={'IO':['sh000300','沪深300指数'],'HO':['sh000016','上证50指数'],'MO':['sh000852','中证1000指数']}[p]
  b=json.dumps({'result':{'status':{'code':0},'data':{'info':{'relate':info[0],'name':info[1],'expire_date':'20'+t[2:4]+'-'+t[4:]+'-20'},'up':[[1,2,3,4,5,6,7,3000,t+'C3000']],'down':[[1,2,3,4,5,6,7,t+'P3000']]}}},ensure_ascii=False).encode(); return R(b)
 p='ho' if '/ho/cffex' in u else 'mo' if '/mo/cffex' in u else 'io'
 return R(('<div id="option_suffix"><ul>'+''.join('<li data-value="'+p+x+'">'+p+x+'</li>' for x in ['2611','2612','2701','2703','2706','2709'])+'</ul></div>').encode())
urllib.request.urlopen=fake

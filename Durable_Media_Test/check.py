import zstd
import requests
import sys
import time
import re
import base64

soap ="""
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ckk="https://CKKDocumentPublishingInterface.dm.billongroup.com/">
   <soapenv:Header/>
   <soapenv:Body>
      <ckk:GetDocument>
         <inParams>
            <documentBlockchainAddress>XXX</documentBlockchainAddress>
            <documentType>PUBLIC</documentType>
         </inParams>
      </ckk:GetDocument>
   </soapenv:Body>
</soapenv:Envelope>
"""



badress = sys.argv[1]
padress = sys.argv[2]
url='http://' + padress
headers = {"content-type": "application/soap+xml"}
soap = soap.replace('XXX', badress)
res = requests.post(url, data=soap, headers=headers)
while not 'PUBLISHING-OK' in res:
    time.sleep(5)
    res = requests.post(url, data=soap, headers=headers).text
    
y=re.search('additionalDetails>.*additionalDetails', res)
s,e = y.start(), y.end()
compressed = res[s+18:e-19]
x2= compressed.encode('ascii')
x3 = base64.b64decode(x2)
x4 = zstd.decompress(x3)

print(x4)

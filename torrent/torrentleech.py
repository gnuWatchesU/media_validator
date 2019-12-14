import re
import urllib.parse
import requests
import lxml.html

# Replace whitespace with periods.
re.sub(r'\s', '.', fn)

urllib.parse.quote(fn)

cookies = dict(member_id='555815', tluid='1305760', tlpass='116ece0259d9dd03695425893ce1945142f49c94', pass_hash='383918eda9855e344053422742890282')
url = 'https://www.torrentleech.org/download/1041865/Thor.-.Ragnarok.2017.BDRip.1080p.AAC.7.1.mp4-LEGi0N.torrent'


r = requests.get(url, cookies=cookies)
tree = lxml.html.fromstring(r.content)


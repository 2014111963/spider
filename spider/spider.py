import os
import sys
import json
import time
import django
import requests
import threading
from lxml import etree
from user_agent import generate_user_agent

path = '/Users/apple/PycharmProjects/sq_spider'
path1 = '/root/sq_spider'
if path not in sys.path:
    sys.path.append(path)
if path1 not in sys.path:
    sys.path.append(path1)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sq_spider.settings")
django.setup()

from main.models import FailedTask, Marker
from company.models import SQ

marker_lock = False
all_count = 0
company_history = []
thread_count = 0


class ProxyConsumer:
    if_banned = False
    faliure_times = 0
    count = 0

    def __init__(self, proxy):
        ip = proxy['ip']
        port = proxy['port']
        self.proxies = {
            "http": f"http://{ip}:{port}",
            "https": f"http://{ip}:{port}"
        }

    def parse(self, resp_text, task_number):
        try:
            html = etree.HTML(resp_text)
            item = {}

            name = html.xpath('//*[(@id = "logoco")]//span')[0].text
            item['number'] = task_number
            item['name'] = name

            contact_dt = html.xpath('//dt')
            contact_dd = html.xpath('//dd')
            for k, dt in enumerate(contact_dt):
                dt_text = dt.text
                dd_text = contact_dd[k].xpath('string(.)')
                if '地址' in dt_text:
                    item['address'] = dd_text
                elif '手机' in dt_text:
                    item['tel'] = dd_text
                elif '固定' in dt_text:
                    item['fixed_telephone'] = dd_text
                elif '邮件' in dt_text:
                    item['email'] = dd_text
                elif '邮政' in dt_text:
                    item['zip_code'] = dd_text
                elif '传真' in dt_text:
                    item['fax'] = dd_text
                elif '经营状态' in dt_text:
                    item['status'] = contact_dd[k].text
                elif '经理' in dt_text or '联系人' in dt_text or '老板' in dt_text or '厂长' in dt_text or '销售' in dt_text \
                        or '负责人' in dt_text or '店长' in dt_text or '职员' in dt_text or '院长' in dt_text or '村长' in dt_text \
                        or '董事长' in dt_text or '业务' in dt_text or '主管' in dt_text or '联系人' in dt_text or '主任' in dt_text \
                        or '总监' in dt_text or '队长' in dt_text or '站长' in dt_text or '局长' in dt_text:
                    item['contact'] = dd_text
                    item['position'] = dt_text[:-1]

            trs = [tr.xpath('string(.)') for tr in html.xpath('//*[(@id = "gongshang")]//td')]
            for k, tr in enumerate(trs):
                if '法人' in tr:
                    item['legal_person'] = trs[k + 1]
                elif '经营产品' in tr:
                    item['productions'] = trs[k + 1]
                elif '经营范围' in tr:
                    item['business_scope'] = trs[k + 1]
                elif '营业执照' in tr:
                    item['business_license_number'] = trs[k + 1]
                elif '成立时间' in tr:
                    item['established'] = trs[k + 1]
                elif '职员人数' in tr:
                    item['number_of_staff'] = trs[k + 1]
                elif '注册资本' in tr:
                    item['capital'] = trs[k + 1]
                elif '经营状态' in tr:
                    item['status'] = trs[k + 1]
                elif '分类' in tr:
                    item['category'] = trs[k + 1]
            SQ.objects.create(**item)

        except Exception as e:
            print('保存的时候报错', e, task_number)
            FailedTask.objects.create(number=task_number)

    def get_a_task(self):
        global marker_lock
        while True:
            if not marker_lock:
                marker_lock = True
                if Marker.objects.exists():
                    marker: Marker = Marker.objects.first()
                    marker_number = marker.marker
                    marker.marker = marker_number + 1
                    marker.save()
                    task_number = marker_number
                else:
                    Marker.objects.create(marker=1)
                    task_number = 1
                marker_lock = False
                break
            else:
                time.sleep(0.005)
        return task_number

    def get_resp_text(self):
        global all_count
        global company_history
        global thread_count

        thread_count += 1
        task_number = self.get_a_task()
        url = f'http://www.11467.com/qiye/{task_number}.htm'
        headers = {'User-Agent': generate_user_agent(),
                   "Connection": "keep-alive",
                   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                   "Accept-Language": "zh-CN,zh;q=0.8"}
        try:
            resp_text = str(requests.get(url, headers=headers, timeout=4, proxies=self.proxies).text)
            self.faliure_times = 0
            if '太快了' in resp_text:
                self.if_banned = True
                print('太快了', self.proxies)
                FailedTask.objects.create(number=task_number)
                thread_count -= 1
                return
            elif '没找到' in resp_text:
                self.count += 1
                company_history.append(time.time())
                print('没找到', url)
            else:
                self.count += 1
                company_history.append(time.time())
                print('成功抓取一个', url)
                self.parse(resp_text, task_number=task_number)
            if len(company_history) > 10:
                print(len(company_history) / (company_history[-1] - company_history[0]))
        except Exception as e:
            print(e)
            self.faliure_times += 1
            FailedTask.objects.create(number=task_number)
        thread_count -= 1
        print(thread_count)

    def consume_a_proxy(self):
        global thread_count
        for i in range(60):
            if self.if_banned or self.faliure_times >= 2 or thread_count > 50:
                break
            else:
                threading.Thread(target=self.get_resp_text).start()
                time.sleep(7)
        print(f'这个代理总共抓取了:{self.count}个网页')


class Spider:

    def get_proxies(self):
        print('从站大爷获取新代理：----------------------------------')
        try:
            proxies = json.loads(requests.get(
                'http://www.zdopen.com/ShortProxy/GetIP/?api=201906181828154211&akey=e25682faa21f6792&order=1&type=3').text)[
                'data'][
                'proxy_list']
        except Exception as e:
            print(f'站大爷出错了，报错信息是{e}')
            return []
        return proxies

    def run(self):
        global company_history
        time.sleep(2)
        while True:
            if len(company_history) > 50:
                company_history = company_history[-50:-1]
            proxies = self.get_proxies()
            for proxy in proxies:
                proxy_consumer = ProxyConsumer(proxy=proxy)
                threading.Thread(target=proxy_consumer.consume_a_proxy).start()
            time.sleep(10)


if __name__ == '__main__':
    spider = Spider()
    spider.run()

import requests,math,time
import threading
import queue
import time
from db.redis_oper import redis_oper
from db.mysql_oper import mysql_oper
from lxml import  etree
from Log.log import log_crawler


class crawler(object):
    def __init__(self):
        self.redis_oper=redis_oper()
        self.prefix_url='https://bj.lianjia.com'
        self.headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
        self.total_page='//div[@class="total fl"]/span/text()'
        self.house_urls_xpath='//ul[@class="listContent"]/li/a/@href'
        self.distinct_xpath='//div[@data-role="ershoufang"]/div/a/@href'
        self.area_xpath='//div[@data-role="ershoufang"]/div[2]/a/@href'
        self.a1_xpath='//dl[@class=" hasmore"][1]/dd/a/@href'
        self.a1a2_xpath='//dl[@class=" hasmore"][2]/dd/a/@href'
        self.a1a2a3_xpath='//dl[@class=" "]//dd/a/@href'
        self.y1_xpath='//div[@class="list-more"]/dl[7]/dd/a/@href'
        self.xpath_dict={0:self.distinct_xpath,1:self.area_xpath,2:self.a1_xpath,
                         3:self.a1a2_xpath,4:self.a1a2a3_xpath,5:self.y1_xpath}
        self.mutex=threading.Lock()
        self.save_q=queue.Queue()
        self.out=False
        self.exit_index_flag=False

        self.exit_save_flag=False
    def complete_legal_url(self,url):
        if str(url).startswith("http"):
            return str(url)
        elif str(url).startswith("javascript"):
            log_crawler.debug("%s is not legal")
            return
        return 'https://bj.lianjia.com'+str(url)

    def filter_urls(self):
        while self.redis_oper.node_not_empty():
            if self.out:
                log_crawler.debug("exit by main thread")
                break
            xpath_index, node_url = self.redis_oper.lpop_node_urls_queue()
            log_crawler.debug("%s get", node_url)
            try:
                full_node_url=self.complete_legal_url(node_url)
                if not full_node_url:
                    continue
                xpath_str=self.xpath_dict.get(xpath_index)
                retries=10
                while retries:
                    response=requests.get(full_node_url,headers=self.headers)
                    raw_html=response.text
                    html=etree.HTML(raw_html)
                    total_num=html.xpath(self.total_page)[0]
                    page_num = math.ceil(float(total_num) / 30)
                    if page_num == 0:
                        retries -= 1
                        if retries==0:
                            log_crawler.debug("retries "+full_node_url)
                        continue
                    else:
                        house_urls = html.xpath(self.house_urls_xpath)
                        self.redis_oper.rpush_house_urls_queue(*house_urls)
                        if page_num>100:
                            xpath_index+=1
                            next_node_urls=html.xpath(xpath_str)
                            self.redis_oper.rpush_node_urls_queue(xpath_index,next_node_urls)
                        elif 1<=page_num<=100:
                            index_urls=[full_node_url+"pg{}".format(pn) for pn in range(2,page_num+1)]
                            self.redis_oper.rpush_index_urls_queue(*index_urls)
                    break
            except:
                log_crawler.error("%s push back to node_url_queue",node_url,exc_info=True)
                self.redis_oper.lpush_node_urls_queue(xpath_index,node_url)
                break

    def get_house_urls(self):
        while not self.exit_index_flag or self.redis_oper.index_urls_not_empty():
            if self.out:
                log_crawler.debug("exit by main thread")
                break
            index_url=self.redis_oper.lpop_index_urls_queue()
            if not index_url:
                time.sleep(1)
                continue
            log_crawler.debug(index_url + " get")
            retries=7
            while retries:
                try:
                    response=requests.get(index_url,headers=self.headers)
                    raw_html=response.text
                    html=etree.HTML(raw_html)
                    house_urls=html.xpath(self.house_urls_xpath)
                    if not house_urls:
                        log_crawler.debug("reties %s",index_url)
                        retries-=1
                        continue
                    self.redis_oper.rpush_house_urls_queue(*house_urls)
                    break
                except:
                    log_crawler.error("%s push back to index_urls_queue",index_url,exc_info=True)
                    self.redis_oper.rpush_index_urls_queue(index_url)
                    break
    def get_tran_info(self):
        print(self.redis_oper.house_urls_not_empty())
        while self.redis_oper.house_urls_not_empty():
            if self.out:
                log_crawler.debug("exit by main thread")
                break
            house_url = self.redis_oper.lpop_house_urls_queue()
            log_crawler.debug("%s get",house_url)
            retries=5
            while retries:
                try:
                    response=requests.get(house_url,headers=self.headers)
                    raw_html=response.text
                    params=self.get_filled_values(house_url,raw_html)
                    self.save_q.put(params)
                except:
                    retries-=1
                    if retries==0:
                        log_crawler.error("push back to house_urls_queue %s", house_url, exc_info=True)
                        self.redis_oper.rpush_house_urls_queue(house_url)
                else:
                    break


    def parse_house_page(self,html,xpath):
        target_element=html.xpath(xpath)
        if target_element:
            return float(target_element[0])
        return
    def get_filled_values(self,url,raw_html):
        html=etree.HTML(raw_html)
        trans_time = html.xpath('//div[@class="house-title LOGVIEWDATA LOGVIEW"]/div[1]/span/text()')
        if trans_time:
            trans_time = trans_time[0].split(" ")[0]
        total_price = self.parse_house_page(html, '//span[@class="dealTotalPrice"]/i/text()')
        av_price = self.parse_house_page(html, '//div[@class="price"]/b/text()')
        basic_info=html.xpath('//div[@class="msg"]/span/label/text()')
        if total_price:
            basic_info_list=list(map(lambda x:float(x.strip()) if x!="暂无数据" else None, basic_info))
        else:
            basic_info_list=[None]*6
        basic_property = html.xpath('//div[@class="base"]/div[2]/ul/li/text()')
        for i,e in enumerate(basic_property):
            basic_property[i]=e.strip()
            if e.strip()=="暂无数据":
                log_crawler.debug(str(basic_property))
                basic_property[i]=None
            elif e.strip()[-1:]=="㎡":
                basic_property[i]=float(e.strip()[:-1])
        listing_date=html.xpath('//div[@class="transaction"]/div[2]/ul/li[3]/text()')[0].strip()
        geo_info=list(map(lambda x:x.strip()[:3],html.xpath
        ('//div[@class="deal-bread"]/a[position()>=2 and position()<=4]/text()')))
        params=[trans_time,total_price,av_price]+basic_info_list+basic_property+[listing_date]+geo_info+[url]
        return params

    def insert2mysql(self):
        m_oper=mysql_oper()
        ammount=0
        while True:
            if (self.out or self.exit_save_flag) and self.save_q.empty():
                m_oper.commit()
                m_oper.close()
                log_crawler.debug("commit to mysql succcessfully")
                break
            try:
                params=self.save_q.get(timeout=1)
                log_crawler.debug("%s get", params)
            except queue.Empty:
                pass
            else:
                try:
                    m_oper.replace(params)
                    self.redis_oper.add_house_urls_set(params[-1])
                    ammount+=1
                    if ammount%100==0:
                        m_oper.commit()
                    log_crawler.debug("insert to mysql %s",params)
                except:
                    log_crawler.error("failed insert %s",str(params),exc_info=True)
    def main(self):
        try:
            tl_index = []
            tl_house = []
            t_node = threading.Thread(target=self.filter_urls, name="filter")
            t_node.start()
            log_crawler.info("filter thread start")

            for i in range(10):
                t = threading.Thread(target=self.get_house_urls, name="house-{}".format(i))
                t.start()
                tl_index.append(t)
                log_crawler.info("house-{} thread start".format(i))
            log_crawler.info("wait for filter thread stop")
            t_node.join()
            self.exit_index_flag=True
            log_crawler.info("wait for house thread stop")
            for t_index in tl_index:
                t_index.join()


            for i in range(10):
                t = threading.Thread(target=self.get_tran_info, name="tran-{}".format(i))
                tl_house.append(t)
                t.start()
                log_crawler.info("tran-{} thread start".format(i))
            t_insert=threading.Thread(target=self.insert2mysql,name="save")
            t_insert.start()
            log_crawler.info("save thread start")
            log_crawler.info("wait for trans thread stop")

            for t_house in tl_house:
                t_house.join()
            self.exit_save_flag=True
            log_crawler.info("wait for save thread stop")
            t_insert.join()
            log_crawler.info("end")
        except:
            self.out=True
            log_crawler.info("biaji")


if __name__ == '__main__':
    cl=crawler()
    cl.main()



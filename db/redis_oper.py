import redis
from Log.log import log_crawler



class redis_oper(object):
    def __init__(self):
        self.conn=redis.Redis(password="")
        self.node_urls_queue="node_urls_queue"
        self.house_urls_queue="house_urls_queue"
        self.house_urls_set="house_urls_set"
        self.index_urls_queue="index_urls_queue"
        self.node_urls_set="node_urls_set"
        self.seed_url = 'https://bj.lianjia.com/chengjiao/'
        if not self.conn.keys(self.node_urls_queue):
            self.conn.rpush(self.node_urls_queue,[0,self.seed_url])
    def node_not_empty(self):
        return self.conn.keys(self.node_urls_queue)
    def lpop_node_urls_queue(self):
        return eval(self.conn.lpop(self.node_urls_queue))
    def rpush_node_urls_queue(self, num, urls):
        xpath_incre_urls=[]
        for url in set(urls):
            if not self.conn.sismember(self.node_urls_set,url):
                xpath_incre_urls.append([num,url])

        if xpath_incre_urls:
            pipe = self.conn.pipeline()
            pipe.multi()
            pipe.rpush(self.node_urls_queue, *xpath_incre_urls)
            pipe.sadd(self.node_urls_set,*urls)
            pipe.execute()
        else:
            log_crawler.info("dupfilter")


    def lpush_node_urls_queue(self,num,url):
        self.conn.lpush(self.node_urls_queue, [num,url])

    def index_urls_not_empty(self):
        return self.conn.keys(self.index_urls_queue)
    def lpop_index_urls_queue(self):
        index_url=self.conn.lpop(self.index_urls_queue)
        if index_url:
            return index_url.decode()

    def rpush_index_urls_queue(self,*urls):
        pipe = self.conn.pipeline()
        if urls:
            pipe.multi()
            pipe.rpush(self.index_urls_queue, *urls)
            pipe.execute()

    def house_urls_not_empty(self):
        return self.conn.keys(self.house_urls_queue)
    def lpop_house_urls_queue(self):
        house_url=self.conn.lpop(self.house_urls_queue)
        if house_url:
            return house_url.decode()
    def rpush_house_urls_queue(self,*urls):
        increase_urls=[]
        for url in urls:
            if not self.url_in_house_urls_set(url):
                increase_urls.append(url)

        if increase_urls:
            log_crawler.debug(increase_urls)
            pipe = self.conn.pipeline()
            pipe.multi()
            pipe.rpush(self.house_urls_queue,*increase_urls)
            pipe.sadd(self.house_urls_set,*increase_urls)
            pipe.execute()
    def add_house_urls_set(self,url):
        self.conn.sadd(self.house_urls_set,url)
    def url_in_house_urls_set(self,url):
        return self.conn.sismember(self.house_urls_set,url)


if __name__ == '__main__':
    rd_oper=redis_oper()
    print(rd_oper.url_in_house_urls_set("https://bj.lianjia.com/chengjiao/101103325037.html"))


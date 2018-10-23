import pymysql
class mysql_oper(object):
    def __init__(self):
        self.conn=pymysql.connect(user='js',password='',db='LianJia')
    def insert(self,params):

        with self.conn.cursor() as cur:
            cur.execute("insert into transaction values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",params)
    def replace(self,params):
        with self.conn.cursor() as cur:
            cur.execute("replace into transaction values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",params)
    def commit(self):
        self.conn.commit()
    def close(self):
        self.conn.close()

if __name__ == '__main__':
    msql_oper=mysql_oper()

    a=['2018.02', None, None, None, None, None, None, None, None, '3室1厅1厨2卫', '中楼层 (共5层)', 135.0, '平层', None, '板楼', '东南 南 西南 西 西北', '2003', '其他', '混合结构', '自供暖', '一梯两户', '70年', '无', '2018-04-23', '北京二', '朝阳二', '十八里', 'https://bj.lianjia.com/chengjiao/BJ0004009942.html']

    msql_oper.replace(a)
    msql_oper.commit()
    msql_oper.close()



import datetime
#from datetime import datetime, timedelta
import time
import urllib.request
import xml.etree.ElementTree as etree
#import winsound #비프음

#from xml.etree import ElementTree

class Station:
    def __init__(self, routeid, stationid):
        self.route_id = routeid
        self.station_id= stationid
        self.soonbus_pno1 = 'empty'
        self.bus_list =[]
        self.sta_order = ' '

    def get_arrivalurl(self):
        #key_1st : 1hB9hu7PVZ53bSDFmXE93%2BAiKs02dU%2Fh8eOztPjSXdeLidXoFOrYH%2BI3dArIwRwGHfa3Ocd33sqnHMwXppUkTg%3D%3D
        #key_2nd : WoH9KBrhycDNHH2BVJMHdNYfHUK3zUbY7aztWSuh2TwMO5mWuIGaOGsmqCNbHPFhvl7AK6Sz01tAjLWy8vNp7g%3D%3D
        #key_1st_server : MDiOQsHI0cvIm%2F8hWTuM09vWbjC3vqNypbk2QXffMnTSP6a%2FR0YXBYAQj3bPhdLPId8dXDP7olmwhegJWzuKqw%3D%3D

        key = 'MDiOQsHI0cvIm%2F8hWTuM09vWbjC3vqNypbk2QXffMnTSP6a%2FR0YXBYAQj3bPhdLPId8dXDP7olmwhegJWzuKqw%3D%3D'
        url = "http://openapi.gbis.go.kr/ws/rest/busarrivalservice?serviceKey=%s&routeId=%s&stationId=%s" % (
            key, self.route_id, self.station_id)
        data = urllib.request.urlopen(url).read()
        time.sleep(0.1)

        filename = "%s_busarrival.xml" % self.route_id
        f = open(filename, 'wb')
        f.write(data)
        f.close()

        tree = etree.parse(filename)
        root = tree.getroot()
        return tree

    def extract_bus_info(self, tree):

        busarrivalitem = tree.find('msgBody/busArrivalItem')

        qtime = tree.findtext('msgHeader/queryTime')
        flag = busarrivalitem.find('flag').text
        locno1 = busarrivalitem.find('locationNo1').text
        locno2 = busarrivalitem.find('locationNo2').text
        pno1 = busarrivalitem.find('plateNo1').text
        pno2 = busarrivalitem.find('plateNo2').text
        pred1 = busarrivalitem.find('predictTime1').text
        pred2 = busarrivalitem.find('predictTime2').text
        seat1 = busarrivalitem.find('remainSeatCnt1').text
        seat2 = busarrivalitem.find('remainSeatCnt2').text
        routeid = busarrivalitem.find('routeId').text
        stationid = busarrivalitem.find('stationId').text
        station_order = busarrivalitem.find('staOrder').text

        self.sta_order = station_order
        dt = datetime.datetime.now()
        kor = dt + datetime.timedelta(hours=9)

        d_q_time = datetime.datetime.strptime(qtime, '%Y-%m-%d %H:%M:%S.%f')
        d_time = str(d_q_time.hour) + str(':') + str(d_q_time.minute) + str(':') + str(d_q_time.second)

        pred_diff = None
        if pred1 != None and pred2 != None:
            pred_diff = int(pred2) - int(pred1) #가능한가 확인, 시간도... localtime이나 아니면 datetime.now()로... 되면 attributename도 추가하기.
###                 0       1                 2     3           4   5       6       7       8   9       10      11      12      13                        14  15         16  17       18
        bus_info = [qtime, kor.weekday(), routeid, stationid, flag, pno1, seat1, locno1, pred1, pno2, seat2, locno2, pred2, int(d_q_time.timestamp()), None, pred_diff, 0, d_time, station_order]
### [19] => pred_diff
### [20] =>  해당 정류장에서 실제 타거나 내린 인원 수.

        return bus_info

    def append_buslist(self, newdic):
        self.bus_list.append(newdic)

    def is_newbus(self, tree):  # busarrivialitem is new information, soonbus is new information
        busarrivalitem = tree.find('msgBody/busArrivalItem')

        error_code = tree.findtext('msgHeader/resultCode')
        if error_code != '0':
            if error_code !='4':
                print(tree.findtext('comMsgHeader/errMsg'))
                return False


        if self.soonbus_pno1 == 'empty':
            if tree.findtext('msgHeader/resultCode') == '4':
                return False
            elif busarrivalitem == None:
                return False
            elif busarrivalitem.findtext('locationNo1') == '1':
                return True
            else:
                return False

        if tree.findtext('msgHeader/resultCode') == '4':
            return True
        elif self.soonbus_pno1 != busarrivalitem.findtext('plateNo1'):
            return True
        else:
            return False
        return False

    def operate(self):
        try:
            tree = self.get_arrivalurl()
        except:
            time.sleep(0.2)
            try:
                tree = self.get_arrivalurl()
            except :
                return False

        if tree.findtext('msgHeader/resultCode') == '8':
            tree = self.get_arrivalurl()
        if tree.findtext('msgHeader/resultCode') == '8':
            tree = self.get_arrivalurl()


        try :
            if len(self.bus_list) >= 1:
                if '1' == tree.findtext('msgBody/busArrivalItem/locationNo1'):#다가오는 버스 위치가 1인지확인
                    if (self.bus_list[-1][7] == '1') and (self.bus_list[-1][5] == tree.findtext('msgBody/busArrivalItem/plateNo1')): #마지막 버스 위치가 1이고 다가오는 버스가 똑같은 경우인경우.
                        return False
                    if self.bus_list[-1][5] == tree.findtext('msgBody/busArrivalItem/plateNo1'):# 다가오는 버스가 마지막 버스와 같은 경우
                        if self.bus_list[-1][16] == 1:
                            self.bus_list[-1] = self.extract_bus_info(tree)
                            self.bus_list[-1][16] = 1
                            return False
                        self.bus_list[-1] = self.extract_bus_info(tree)
                        return False
        except :
            print("it's okay")

        if self.is_newbus(tree):
            if len(self.bus_list) >= 2:
                if self.bus_list[-1][16] == 0:  # 시간이 한번도 안바꼈으면 바꾼다.
                    self.bus_list[-1][0] = tree.findtext('msgHeader/queryTime')
                    self.bus_list[-1][13] = int(time.time())
                    self.bus_list[-1][16] = 1
                    d_q_time = datetime.datetime.strptime(self.bus_list[-1][0], '%Y-%m-%d %H:%M:%S.%f')
                    d_time = str(d_q_time.hour) + str(':') + str(d_q_time.minute) + str(':') + str(d_q_time.second)
                    self.bus_list[-1][17] = d_time
                self.bus_list[-1][14] = self.bus_list[-1][13] - self.bus_list[-2][13]

            elif len(self.bus_list) == 1:
                if self.bus_list[0][16] == 0:  # 시간이 한번도 안바꼈으면 바꾼다.
                    self.bus_list[0][0] = tree.findtext('msgHeader/queryTime')
                    self.bus_list[0][13] = int(time.time())
                    self.bus_list[0][16] = 1
                    d_q_time = datetime.datetime.strptime(self.bus_list[-1][0], '%Y-%m-%d %H:%M:%S.%f')
                    d_time = str(d_q_time.hour) + str(':') + str(d_q_time.minute) + str(':') + str(d_q_time.second)
                    self.bus_list[-1][17] = d_time

            if tree.findtext('msgHeader/resultCode') == '4':  # 뒷차가 없는경우.
                self.soonbus_pno1 = 'empty'

            else:
                newbus = self.extract_bus_info(tree)
                self.append_buslist(newbus)
                self.soonbus_pno1 = newbus[5]

            """
            if tree.findtext('msgHeader/resultCode') == '4':  # 뒷차가 없는경우.
                if len(self.bus_list) >= 2:
                    if self.bus_list[-1][16] == 0: # 시간이 한번도 안바꼈으면 바꾼다.
                        self.bus_list[-1][0] = tree.findtext('msgHeader/queryTime')
                        self.bus_list[-1][13] = int(time.time())
                        self.bus_list[-1][16] = 1
                    self.bus_list[-1][14] = self.bus_list[-1][13] - self.bus_list[-2][13]

                elif len(self.bus_list) == 1:
                    if self.bus_list[0][16] == 0: # 시간이 한번도 안바꼈으면 바꾼다.
                        self.bus_list[0][0] = tree.findtext('msgHeader/queryTime')
                        self.bus_list[0][13] = int(time.time())
                        self.bus_list[0][16] = 1

                self.soonbus_pno1 = 'empty'

            else:

                if len(self.bus_list) >= 2:
                    if self.bus_list[-1][16] == 0: # 시간이 한번도 안바꼈으면 바꾼다.
                        self.bus_list[-1][0] = tree.findtext('msgHeader/queryTime')
                        self.bus_list[-1][13] = int(time.time())
                        self.bus_list[-1][16] = 1
                    self.bus_list[-1][14] = self.bus_list[-1][13] - self.bus_list[-2][13]

                elif len(self.bus_list) == 1:
                    if self.bus_list[0][16] == 0: # 시간이 한번도 안바꼈으면 바꾼다.
                        self.bus_list[0][0] = tree.findtext('msgHeader/queryTime')
                        self.bus_list[0][13] = int(time.time())
                        self.bus_list[0][16] = 1

                newbus = self.extract_bus_info(tree)
                self.append_buslist(newbus)
                self.soonbus_pno1 = newbus[5]
            """
        else:
            return False


def make_file(bus_id, station_list, flag=0):  # 마지막 정류소까지 잘 저장되는지 확인
    dt = datetime.datetime.now()
    kor = dt + datetime.timedelta(hours=9)

    if kor.hour >=0 and kor.hour <=3:
        kor = dt

    filename = "/home/ec2-user/cov/bus3/%s/eachday_wholedata_w/%s_%s_%s_w.txt" % (
    bus_id, bus_id, kor.date(), kor.weekday())

    f = open(filename, 'w')
    attribute_name = "[time], [day], [routeid], [stationid], [flag], [pno1], [seat1], [loctno1], [pred1], [pno2], [seat2], [loctno2], [pred2], [seconds], [seconds_diff], [pred_min_diff], [ignore_flag], [d_time], [staOrder]\n"
    f.write(attribute_name)


    idx = 0
    while (idx < len(station_list)):
        f.write(str(station_list[idx].bus_list))
        f.write("\n\n")
        idx+=1

    f.close()

    idx = 0
    while (idx < len(station_list)):
        if flag == 0:
            station_filename = "/home/ec2-user/cov/bus3/%s/dataplus_temp_w/%s_%s_%s_%s_%s_w_dataplus_temp.txt" % (
            bus_id, bus_id, kor.date(), kor.weekday(), station_list[idx].sta_order, station_list[idx].station_id)
            station_f = open(station_filename, 'w')

            station_filename2 = "/home/ec2-user/cov/bus3/%s/data_temp_w/%s_%s_%s_%s_%s_w_data_temp.txt" % (
            bus_id, bus_id, kor.date(), kor.weekday(), station_list[idx].sta_order, station_list[idx].station_id)
            station_f2 = open(station_filename2, 'w')

            for k in range(len(station_list[idx].bus_list)):
                #임시, 날짜별 정료소별, 버스번호, 시간, 요일, 기다린시간, 다음버스예상기다린시간, 잔여좌석
                station_f.write(station_list[idx].bus_list[k][5])#차번호
                station_f.write('\t')
                station_f.write(station_list[idx].bus_list[k][0])#요청시간
                station_f.write('\t')
                station_f.write(station_list[idx].bus_list[k][17])#H:M:S
                station_f.write('\t')
                station_f.write(str(station_list[idx].bus_list[k][1]))#요일
                station_f.write('\t')
                station_f.write(str(station_list[idx].bus_list[k][14]))#실제기다린시간
                station_f.write('\t')
                station_f.write(str(station_list[idx].bus_list[k][15]))#pred_diff
                station_f.write('\t')
                station_f.write(station_list[idx].bus_list[k][6])#잔여좌석
                station_f.write('\n')

                #임시, 날짜별 정류소별, 버스번호, 시간, 요일, 기다린시간, 잔여좌석
                station_f2.write(station_list[idx].bus_list[k][5])
                station_f2.write('\t')
                station_f2.write(station_list[idx].bus_list[k][0])
                station_f2.write('\t')
                station_f2.write(station_list[idx].bus_list[k][17])
                station_f2.write('\t')
                station_f2.write(str(station_list[idx].bus_list[k][1]))
                station_f2.write('\t')
                station_f2.write(str(station_list[idx].bus_list[k][14]))
                station_f2.write('\t')
                station_f2.write(station_list[idx].bus_list[k][6])
                station_f2.write("\n")
            station_f.close()
            station_f2.close()
        elif flag == 1:
            station_filename = "/home/ec2-user/cov/bus3/%s/dataplus_a/%s_%s_%s_a_dataplus.txt" % (
            bus_id, bus_id, station_list[idx].sta_order, station_list[idx].station_id)
            station_f = open(station_filename, 'a')

            station_filename2 = "/home/ec2-user/cov/bus3/%s/data_a/%s_%s_%s_a_data.txt" % (
                bus_id, bus_id, station_list[idx].sta_order, station_list[idx].station_id)
            station_f2 = open(station_filename2, 'a')


            for k in range(len(station_list[idx].bus_list)):
                #모든날짜 정료소별, 버스번호, 시간, 요일, 기다린시간, 다음버스예상기다린시간, 잔여좌석
                station_f.write(station_list[idx].bus_list[k][5])
                station_f.write('\t')
                station_f.write(station_list[idx].bus_list[k][0])
                station_f.write('\t')
                station_f.write(station_list[idx].bus_list[k][17])
                station_f.write('\t')
                station_f.write(str(station_list[idx].bus_list[k][1]))
                station_f.write('\t')
                station_f.write(str(station_list[idx].bus_list[k][14]))
                station_f.write('\t')
                station_f.write(str(station_list[idx].bus_list[k][15]))
                station_f.write('\t')
                station_f.write(station_list[idx].bus_list[k][6])
                station_f.write('\n')

                # 임시, 날짜별 정류소별, 버스번호, 시간, 요일, 기다린시간, 잔여좌석
                station_f2.write(station_list[idx].bus_list[k][5])
                station_f2.write('\t')
                station_f2.write(station_list[idx].bus_list[k][0])
                station_f2.write('\t')
                station_f2.write(station_list[idx].bus_list[k][17])
                station_f2.write('\t')
                station_f2.write(str(station_list[idx].bus_list[k][1]))
                station_f2.write('\t')
                station_f2.write(str(station_list[idx].bus_list[k][14]))
                station_f2.write('\t')
                station_f2.write(station_list[idx].bus_list[k][6])
                station_f2.write("\n")

            station_f.close()
            station_f2.close()

        detail_w_station_filename = "/home/ec2-user/cov/bus3/%s/eachday_w/%s_%s_%s_%s_%s_w_eachday.txt" % (
            bus_id, bus_id, kor.date(), kor.weekday(),station_list[idx].sta_order, station_list[idx].station_id)
        d_w_station = open(detail_w_station_filename, 'w')

        for k in range(len(station_list[idx].bus_list)):
            #정류소별 날짜별, 모든정보.
            d_w_station.write(str(station_list[idx].bus_list[k]))
            d_w_station.write("\n")

        d_w_station.close()

        idx += 1

def make_each_station_file_a(bus_id, station_list):
    idx = 0
    while (idx < len(station_list)):

        detail_a_station_filename = "/home/ec2-user/cov/bus3/%s/wholedata_a/%s_%s_%s_a_whole.txt" % (
        bus_id, bus_id, station_list[idx].sta_order, station_list[idx].station_id)
        d_station = open(detail_a_station_filename, 'a')

        for k in range(len(station_list[idx].bus_list)):
            for i in range(len(station_list[idx].bus_list[k])):
                # 정류소별 날짜이어지게, 모든 정보
                d_station.write(str(station_list[idx].bus_list[k][i]))
                d_station.write("\t")
            d_station.write("\n")
        d_station.write("\n\n")
        d_station.close()

        idx += 1
"""
def append_pred_differ_seat_differ(station_list):
    idx = 0
    while (idx < len(station_list)):

        for k in range(1, len(station_list[idx].bus_list)):
            station_list[idx].bus_list[k].append(station_list[idx].bus_list[k-1][15])#add pred_differ k=1, bus_list[k][19] 예상기다린시간.

        for k in range(len(station_list[idx].bus_list)):
            if idx+1 < len(station_list):
                if (station_list[idx].bus_list[k][6] != None) and (station_list[idx+1].bus_list[k][6] != None):
                    seat_diff = int(station_list[idx].bus_list[k][6]) - int(station_list[idx+1].bus_list[k][6])# -면 내린사람, +면 탄사람. [k][20] 승차인원
                    station_list[idx].bus_list[k].append(seat_diff)#bus_list[k][20] <- seat_diff

        idx += 1

def make_ml_dataset(bus_id, station_list):
    idx = 0
    while (idx < len(station_list)):
        station_filename = "/home/ec2-user/cov/bus3/%s/dataset_a/%s_%s_%s_a_dataset.txt" % (bus_id, bus_id, station_list[idx].sta_order, station_list[idx].station_id)
        station_f = open(station_filename, 'a')

        for k in range(len(station_list[idx].bus_list)):
            # 임시, 날짜별 정료소별, 버스번호, 시간, 요일, 기다린시간, 다음버스예상기다린시간, 잔여좌석
            station_f.write(station_list[idx].bus_list[k][5])  # 차번호
            station_f.write('\t')
            station_f.write(station_list[idx].bus_list[k][0])  # 요청시간
            station_f.write('\t')
            station_f.write(station_list[idx].bus_list[k][17])  # H:M:S
            station_f.write('\t')
            station_f.write(str(station_list[idx].bus_list[k][1]))  # 요일
            station_f.write('\t')
            station_f.write(str(station_list[idx].bus_list[k][14]))  # 실제기다린시간
            station_f.write('\t')
            station_f.write(str(station_list[idx].bus_list[k][19]))  # pred_diff
            station_f.write('\t')
            station_f.write(station_list[idx].bus_list[k][6])  # 잔여좌석
            station_f.write('\t')
            station_f.write(str(station_list[idx].bus_list[k][20]))  # 실제 탑승인원수
            station_f.write('\n')

        station_f.close()

"""
def main():
    m4101_routeid = 234000875# M4101
    m4101_stationid = [228000950, 228000911, 228000883, 206000230, 101000002, 100000001, 101000141,
                         101000264, 101000026, 101000114, 101000148, 101000001, 206000220, 228000905,
                         228000920, 228002278]
    m7119_routeid = 218000015  # m7119
    m7119_stationid = [219000711, 219000457, 219000714, 219000356, 219000370, 112000012, 112000016,
                       100000034, 101000128, 101000022, 101000020, 112000017, 112000013, 219000383]
    m7106_routeid = 218000012  # m7106
    m7106_stationid = [219000363, 219000192, 219000356, 219000370, 219000368, 112000012, 112000016,
                       100000034, 101000128, 101000022, 101000020, 112000017, 112000013, 219000385, 219000383]
    m7111_routeid = 229000102  # m7111
    m7111_stationid = [229001545, 229001553, 229001422, 229001431, 229001501, 112000012, 112000016, 100000034, 101000128, 101000022, 112000017, 112000013, 229001502]

    m6117_routeid = 232000071  # m6117
    m6117_stationid = [232000725, 232000741, 232000545, 232000553, 232000758, 232000727, 113000424, 113000422,
                       113000420, 113000419, 112000365, 101000250, 112000170, 113000417, 113000416, 113000414,
                       113000412]
    m5121_routeid = 234001317  # m5121
    m5121_stationid = [203000023, 203000116, 203000069, 203000294, 203000399, 203000042, 101000002, 101000058,
                       101000128, 101000008, 101000006, 101000148, 101000001]

    m5107_routeid = 234001243  # m5107
    m5107_stationid = [203000157, 203000123, 203000122, 203000120, 228000700,
                       101000002, 101000058, 101000128, 101000008, 101000006, 101000148, 101000001, 228000679]

    m4108_routeid = 234001245  # m4108
    m4108_stationid = [233001450, 233001224, 233001219, 233001562, 233001281, 101000002, 101000058,
                       101000128, 101000008, 101000006, 101000148, 101000001, 233001282]
    m4102_routeid = 234001159  # m4012
    m4102_stationid = [206000087, 206000725, 206000299, 206000613, 101000002, 100000001, 101000141, 101000264,
                       101000026, 101000114, 101000148, 101000001, 206000239]

    m4403_routeid = 234000995 #m4403
    m4403_stationid = [233001450, 233001224, 233001219, 233001562, 233001281, 121000086, 121000941, 121000009,
                       121000005, 121000003, 121000220, 233001282]
    ####
####
    time.sleep(9600)
####
    ####
    while(True):

        m4101_station_list = []
        for i in m4101_stationid:
            m4101_station_list.append(Station(m4101_routeid, i))

        m4108_station_list = []
        for i in m4108_stationid:
            m4108_station_list.append(Station(m4108_routeid, i))

        m5107_station_list = []
        for i in m5107_stationid:
            m5107_station_list.append(Station(m5107_routeid, i))

        m4403_station_list = []
        for i in m4403_stationid:
            m4403_station_list.append(Station(m4403_routeid, i))
        #        while ( (time.localtime().tm_min >=0) and (time.localtime().tm_min <= 57)):

        dt = datetime.datetime.now()
        kor = dt + datetime.timedelta(hours=9)
#####        kor = dt+ datetime.timedelta(hours=0)
        while((kor.hour >=4) or (kor.hour<1)):
#        while ((dt.hour >= 00 and dt.minute > 0) and (dt.hour < 1 )):

            dt = datetime.datetime.now()
            kor = dt + datetime.timedelta(hours=9)
####            kor = dt + datetime.timedelta(hours=0)
#            if(kor.hour == 18 and kor.minute >= 57):
#               break
            if kor.hour == 1:
                break
            for k in range(len(m4101_station_list)):
                m4101_station_list[k].operate()

            for k in range(len(m4108_station_list)):
                m4108_station_list[k].operate()

            for k in range(len(m5107_station_list)):
                m5107_station_list[k].operate()

            for k in range(len(m4403_station_list)):
                m4403_station_list[k].operate()

            make_file("m4101", m4101_station_list)
            make_file("m5107", m5107_station_list)
            make_file("m4108", m4108_station_list)
            make_file("m4403", m4403_station_list)

            #Freq = 2500
            #Dur = 1000
            #winsound.Beep(Freq,Dur)
            time.sleep(30) #10초걸림 #서버에서도 얼마나걸리는지확인해보기

        make_file("m4101", m4101_station_list, 1)
        make_file("m5107", m5107_station_list, 1)
        make_file("m4108", m4108_station_list, 1)
        make_file("m4403", m4403_station_list, 1)

        make_each_station_file_a("m4101", m4101_station_list)
        make_each_station_file_a("m5107", m5107_station_list)
        make_each_station_file_a("m4108", m4108_station_list)
        make_each_station_file_a("m4403", m4403_station_list)
        """
        append_pred_differ_seat_differ(m4101_station_list)
        append_pred_differ_seat_differ(m4108_station_list)
        append_pred_differ_seat_differ(m4403_station_list)
        append_pred_differ_seat_differ(m5107_station_list)

        make_ml_dataset("m4101", m4101_station_list)
        make_ml_dataset("m5107", m5107_station_list)
        make_ml_dataset("m4108", m4108_station_list)
        make_ml_dataset("m4403", m4403_station_list)
        """
#############
#############
        print("sleep 3:50")
        time.sleep(13700)#start 04:50
#############
#############
if __name__ == "__main__":
    main()


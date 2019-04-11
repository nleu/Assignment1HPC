from mpi4py import MPI
import  sys, getopt, time,json, os
import operator, math, pprint as p
from itertools import islice
from collections import defaultdict

def read_arguments(argv):
    inputfile = ''
    locationfile = ''
    try:
        opts, args = getopt.getopt(argv,"hi:l:")
    except getopt.GetoptError as error:
        print(error)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            sys.exit()
        elif opt in ("-i"):
            inputfile = arg
        elif opt in ("-l"):
            locationfile = arg

    return inputfile, locationfile

def readFile(input_file, area,comm,list2):
    size = comm.size
    rank = comm.rank
    byteperline = 3550
    filesize = os.path.getsize(input_file)
    linesCount = int(math.ceil(filesize/byteperline))
    limit = int(math.ceil(linesCount/size))
    with open(input_file, encoding="utf8") as f:
        for line in islice(f,limit*(rank),min(limit*(rank+1),linesCount)):
            strLocation = "coordinates\":{\"type\":\"Point\",\"coordinates\":["
            whereIs = line.find(strLocation)
            if whereIs > -1:
                x = [float(i) for i in line[(whereIs+44):(whereIs+70)].split(']')[0].split(',')]              
            else:
                continue
            areaName = ""
            for a in area:
                if x[0] >= a["xmin"] and x[0] <= a["xmax"] and \
                        x[1] >= a["ymin"] and x[1] <= a["ymax"]:
                    areaName = a["id"]
                    break
            if len(areaName) == 0:
                continue
            list2[areaName][areaName] += 1
            strText = "\"text\":\""
            textIs = line.find(strText)
            if textIs > -1:
                l = line[(textIs+8):(textIs+148)].split('",')[0].split(' ')[1:-1]
                l = set([k[1:].lower() for k in l if k and k[0]=='#'])
            else:
                continue

            for s in l:
                if len(s)<=33:
                    list2[areaName][s] += 1
    return list2
	def main(argv):
    tic = time.clock()
    input_file,locationfile = read_arguments(argv)
    comm = MPI.COMM_WORLD
    rank = comm.rank
    list2 = dict()
    list3 = dict()
    areaList = []
    with open(locationfile, encoding="utf8") as f:
        area = [j["properties"] for j in json.load(f)["features"]]
        for a in area:
            a["xmin"] = float(a["xmin"])
            a["xmax"] = float(a["xmax"])
            a["ymin"] = float(a["ymin"])
            a["ymax"] = float(a["ymax"])
            list2[a["id"]] = defaultdict(int)
            list3[a["id"]] = defaultdict(int)
            areaList.append(a["id"])
    list2 = readFile(input_file, area,comm,list2)
    list2Combine = comm.gather(list2,root=0)
    if rank ==0:
        for list2C in list2Combine:
            for key2 in list2C:
                for s in list2C[key2]:
                    list3[key2][s] = list3[key2][s] + list2C[key2][s]
        for key2 in list3:
            list3[key2] = {k:v for k, v in list3[key2].items() if not v == 1}
        list1 = {k:v.get(k,0) for k, v in list3.items()}
        print("Total valid tweets: ",sum(list1.values()))
        firstResult = sorted(list1.items(), key=operator.itemgetter(1), reverse=True)
        for r1 in firstResult:
            print(r1[0],":",r1[1],"posts")
        for l2 in firstResult:
            sorted_x = sorted(list3[l2[0]].items(), key=operator.itemgetter(1), reverse=True)
            print(l2[0], end=': ')
            print(sorted_x[1:6])
    print("rank",rank,": ",time.clock()-tic, "s")
if __name__ == "__main__":
    main(sys.argv[1:])


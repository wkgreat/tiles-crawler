'''
Author: Ke Wang
'''
import urllib2
import os
import os.path as path
import time
import pyproj as proj
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED


def main():
    # --Google Imagery Map--
    workspace = "D:\\tiles\\google\\imagery"
    urltem = r"http://mt{a}.google.cn/maps/vt?lyrs=s%40817&hl=zh-CN&gl=CN&x={x}&y={y}&z={z}"
    ext = "jpg"
    referer = r"http://www.gpsov.com/map.php?lang=cn"
    ra = ["1", "2"]

    # --OSM Dark Vector Map--
    # workspace = "D:\\tiles\\osm\\darkmatter"
    # urltem = r"https://maps.tilehosting.com/data/v3/{z}/{x}/{y}.pbf?key=hWWfWrAiWGtv68r8wA6D"
    # ext = "pbf"

    # --OSM Dark Raster Map--
    # workspace = "D:\\tiles\\osm\\darkmatterraster"
    # urltem = r"https://maps.tilehosting.com/styles/darkmatter/{z}/{x}/{y}@2x.png?key=hWWfWrAiWGtv68r8wA6D"
    # ext = "png"

    # --openstreet Humanitarian Map--
    # workspace = "D:\\tiles\\osm\\hot"
    # urltem = r"https://tile-{a}.openstreetmap.fr/hot/{z}/{x}/{y}.png"
    # ext = "png"

    # --openstreet Transprot Map--
    # workspace = "C:\\tiles\\osm\\transprot"
    # urltem = r'https://{a}.tile.thunderforest.com/transport/{z}/{x}/{y}.png?apikey=6170aad10dfd42a38d4d8c709a536f38'
    # ext = "png"

    # --Stamen Toner Map--
    # workspace = "C:\\tiles\\stamen\\toner"
    # urltem = r"http://{a}.tile.stamen.com/toner/{z}/{x}/{y}.png"
    # ext = "png"

    # --Stamen Terrain Map--
    # workspace = "C:\\tiles\\stamen\\terrain"
    # urltem = r"http://{a}.tile.stamen.com/terrain/{z}/{x}/{y}.png"
    # ext = "png"

    # --Exent(BBox)--
    # bbox = [-170, 170, -70, 70] #world
    # bbox = [55.8271,137.8347,0.8293,72.004] #China
    # bbox = [114.8764,119.643799,29.395947,34.650981] #anhui
    # bbox = [114.877201, 118.069588, 32.405768, 33.585753]  # bengbu_fuyang
    # bbox = [116.730431, 118.069588, 32.713815, 33.504415] #bengbu
    # bbox = [116.699870, 118.058910, 38.566189, 40.249798]  # tianjin
    bbox = [113.574970, 118.481580, 24.488552, 30.077726]  # jiangxi

    # ra=["a", "b", "c"]  # multiple urls with different character
    rz = [0, 16]  # zoom level

    max_thread = 100  # max_thread
    max_retry = 30  # max_retry

    the_spider = TileSpider(workspace, urltem, ra=ra, rz=rz, bbox=bbox, ext=ext, maxThread=max_thread)
    the_spider.setReferer(referer)

    the_retry = 0
    while True:
        status = the_spider.batchGetTile()
        the_retry += 1
        if status or the_retry >= max_retry: break
    the_spider.executor.shutdown(wait=True)
    print "Final Status: ", status
    print "ALL FINISHED!"
    raw_input("Please Input <ENTER>:")


class TileSpider:
    header = {
        'User-Agent': 'MMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
        'Cookie': 'AspxAutoDetectCookieSupport=1',
        'Referer': 'http://www.gpsov.com/map.php?lang=cn'
    }
    timeout = 40

    zExtent = {}

    omitZXY = []

    isAllFinished = True

    def __init__(self, workspace, urltem, ra, rz, bbox=(-180, 180, -85, 85), ext='png', maxThread=1):
        self.workspace = workspace
        self.urltem = urltem
        self.ra = ra
        self.rz = rz
        if bbox[2] < -85: bbox[2] = -85
        if bbox[3] > 85: bbox[3] = 85
        self.bbox = bbox
        self.ext = ext
        self.maxThread = maxThread
        self.executor = ThreadPoolExecutor(max_workers=self.maxThread)
        self.results = []

    def setReferer(self, referer):
        if referer.strip():
            self.header["Referer"] = referer

    def _getTile(self, url):
        request = urllib2.Request(url, None, self.header)
        response = urllib2.urlopen(request, None, self.timeout)
        return response

    def getAndSaveTile(self, url, thePath, factor, remain):
        # print url + " -> " + thePath
        try:
            f = None
            response = self._getTile(url)
            f = open(thePath, "wb")
            f.write(response.read())
        except Exception, e:
            if f is not None:
                f.close()
            if path.exists(thePath):
                os.remove(thePath)
            time.sleep(4)
            self.batchGetTile(factor, remain)
        finally:
            if f is not None:
                f.close()

    def getAndSaveTileMulti(self, url, thePath):
        # print url + " -> " + thePath
        try:
            f = None
            response = self._getTile(url)
            f = open(thePath, "wb")
            f.write(response.read())
        except Exception, e:
            self.isAllFinished = False
            print "ERROR: " + url + " -> " + thePath, e.message
            if f is not None:
                f.close()
            if path.exists(thePath):
                os.remove(thePath)
        finally:
            if f is not None:
                f.close()

    def batchGetTile(self):

        for z in xrange(self.rz[0], self.rz[1] + 1):

            if self.zExtent.has_key(z):
                x0, x1, y0, y1 = self.zExtent[z]
            else:
                self.zExtent[z] = getXYZRangeByLatLon(z, *self.bbox)
                x0, x1, y0, y1 = self.zExtent[z]
            print z, x0, x1, y0, y1
            i = 0
            counter = 0
            for x in xrange(x0, x1 + 1):
                for y in xrange(y0, y1 + 1):
                    counter = (counter + 1) % 1000
                    if counter == 0: print z, x0, '-', x, '-', x1, ' ', y0, '-', y, '-', y1
                    lenra = len(self.ra)
                    if lenra > 0:
                        i = (i + 1) % lenra
                        a = self.ra[i]
                        # url = self.urltem % (a, z, x, y)
                        url = self.urltem.replace("{a}", a).replace("{x}", str(x)).replace("{y}", str(y)).replace("{z}",
                                                                                                                  str(
                                                                                                                      z))
                        thePath = self.getPath(self.workspace, z, x, y, self.ext)
                    else:
                        # url = self.urltem % (z, x, y)
                        url = self.urltem.replace("{x}", str(x)).replace("{y}", str(y)).replace("{z}", str(z))
                        thePath = self.getPath(self.workspace, z, x, y, self.ext)
                    if thePath is None: continue
                    self.results.append(self.executor.submit(self.getAndSaveTileMulti, url, thePath))
                    self.results = [r for r in self.results if not r.done()]
                    if len(self.results) >= self.maxThread:
                        wait(self.results, return_when=FIRST_COMPLETED)

        return self.isAllFinished

    def getPath(self, workspace, z, x, y, ext):
        dire = path.join(workspace, str(z))
        if not path.exists(dire):
            os.makedirs(dire)
        tileName = "%d-%d.%s" % (x, y, ext)
        thePath = path.join(dire, tileName)
        if path.exists(thePath):
            return None
        else:
            return thePath


def e4326t3857(lon, lat):
    e4326 = proj.Proj(init="epsg:4326")
    e3857 = proj.Proj(init="epsg:3857")
    r = proj.transform(e4326, e3857, x=lon, y=lat, radians=False)
    return r


def getXYZRangeByLatLon(z, minlon, maxlon, minlat, maxlat):
    minx, miny = e4326t3857(minlon, minlat)
    maxx, maxy = e4326t3857(maxlon, maxlat)
    return getXYZRangeByXY(z, *(minx, maxx, miny, maxy))


def getXYZRangeByXY(z, x0, x1, y0, y1):
    '''
    According to the characteristics of Mercator Projection,
    the longitude and latitude are sliced equally while cutting tiles.
    '''
    length = 2 ** z
    xLimit = [-20037508.3427892, 20037508.3427892]
    yLimit = [-20037508.3427892, 20037508.3427892]
    xSpan = xLimit[1] - xLimit[0]
    ySpan = yLimit[1] - yLimit[0]
    xstep = xSpan * 1.0 / length
    yStep = ySpan * 1.0 / length

    minx = maxx = miny = maxy = 0
    findminx = False
    findmaxx = False
    findminy = False
    findmaxy = False

    for i in xrange(0, length):
        if findminx and findminy and findmaxx and findmaxy:
            return minx, maxx, miny, maxy
        leftx = xLimit[0] + i * xstep
        rightx = leftx + xstep
        upy = yLimit[1] - i * yStep
        downy = upy - yStep
        if (not findminx) and (leftx <= x0 < rightx):
            findminx = True
            minx = i

        if (not findminy) and (downy <= y1 < upy):
            findminy = True
            miny = i

        if (not findmaxx) and (leftx <= x1 < rightx):
            findmaxx = True
            maxx = i

        if (not findmaxy) and (downy <= y0 < upy):
            findmaxy = True
            maxy = i
    return minx, maxx, miny, maxy


if __name__ == '__main__':
    main()

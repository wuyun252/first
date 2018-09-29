package main

import "common"
import "links_manage/db_operation"
import "links_manage/dnslink"
import "fmt"
import "github.com/gin-gonic/gin"
import (
	"flag"
	"github.com/go-ini/ini"
	"github.com/golang/glog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	cfg      *ini.File
	dbHelper *common.DBHelper
)

var (
	linkMu            sync.RWMutex
	linkDataVersionId int64 = 1
	gLinkIdData       map[linkdb.DnsCoverLinkIdInfo]bool
	gLinkNameData     []linkdb.DnsCoverLinkNameInfo
)

var (
	geoMu   sync.RWMutex
	geoInfo *common.GeoInfo
	idcMu   sync.RWMutex
	idcInfo *dnslink.IdcIdNameMap
)

func updateLinkData(dbHelper *common.DBHelper, timeout time.Duration, validIspIds, validNationIds map[int64]bool) {
	currentLinkData, err := dnslink.GetAllLinks(dbHelper, timeout, validIspIds, validNationIds)
	var currentLinkNameData []linkdb.DnsCoverLinkNameInfo
	geoMu.RLock()
	tmpGeoInfo := geoInfo
	geoMu.RUnlock()
	idcMu.RLock()
	tmpIdcInfo := idcInfo
	idcMu.RUnlock()
	for link := range currentLinkData {
		linkName, err := link.TransformToNameInfo(tmpGeoInfo, tmpIdcInfo.IdcId2Name)
		if err == nil {
			currentLinkNameData = append(currentLinkNameData, linkName)
		}
	}
	linkMu.Lock()
	defer linkMu.Unlock()
	if err != nil {
		// init failed, exit
		if len(gLinkIdData) == 0 {
			glog.Fatal("init link data failed")
		} else {
			glog.Error("update link data failed")
		}
	} else {
		// judge if changed
		if len(currentLinkData) != len(gLinkIdData) {
			linkDataVersionId += 1
		} else {
			for link := range currentLinkData {
				if !gLinkIdData[link] {
					linkDataVersionId += 1
					break
				}
			}
		}
		gLinkIdData = currentLinkData
		gLinkNameData = currentLinkNameData
	}
}

// scan through all the oss to load geo info
func loadGeoInfo(timeout time.Duration) (*common.GeoInfo, error) {
	ossDbs, err := linkdb.GetClusterOssIps(dbHelper)
	if err != nil {
		return nil, fmt.Errorf("get cluster oss ip failed")
	}
	geoUrlFormat := "http://%s/cgi-bin/tars_oss/cgi-bin/get_cdns_basic_info_int.cgi?info_type=nation,area,province,isp&user=cloudywu"
	for _, ossIpPair := range ossDbs {
		ipPair := []string{ossIpPair.Master, ossIpPair.Slaver}
		for _, ossIp := range ipPair {
			geoUrl := fmt.Sprintf(geoUrlFormat, ossIp)
			newGeoInfo, err := common.LoadGeoInfo(geoUrl, timeout)
			if err == nil {
				return newGeoInfo, nil
			}
		}
	}
	return nil, fmt.Errorf("get geo info from all oss failed")
}

func loadValidIdMap(cfg *ini.File, keyName string) (map[int64]bool, error) {
	validIdString := strings.Split(cfg.Section("server").Key(keyName).String(), ",")
	validIds := make(map[int64]bool)
	for _, val := range validIdString {
		vId, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			glog.Errorf("convert  id failed %v", val)
			return nil, fmt.Errorf("covert %s to int failed", val)
		}
		validIds[vId] = true
	}
	return validIds, nil
}

func init() {
	flag.Parse()
	var err error

	// load config
	cfg, err = ini.Load("conf/server.ini")
	if err != nil {
		glog.Fatal("load config failed: ", err.Error())
	}

	// init db helper
	var dbConf = common.NewDBConf(cfg.Section("DBConf").Key("host").String(), cfg.Section("DBConf").Key("db").String(),
		cfg.Section("DBConf").Key("user").String(), cfg.Section("DBConf").Key("passwd").String(),
		cfg.Section("DBConf").Key("charset").String(), cfg.Section("DBConf").Key("port").MustInt(3306),
		cfg.Section("DBConf").Key("maxOpenConns").MustInt(10), cfg.Section("DBConf").Key("maxIdleConns").MustInt(5),
		time.Second*cfg.Section("DBConf").Key("maxLifeTimeSeconds").MustDuration(10))
	dbHelper = common.InitDBHelper(dbConf)
	if dbHelper == nil {
		glog.Fatal("init DB helper failed ")
	}

	// init geo info
	cgiTimeoutSeconds := time.Second * cfg.Section("cgi").Key("timeout").MustDuration(5)
	geoInfo, err = loadGeoInfo(cgiTimeoutSeconds)
	if err != nil {
		glog.Fatal("load geo info failed")
	}

	// update geo info
	updateGeoPeriod := time.Second * cfg.Section("server").Key("geoUpdatePeriod").MustDuration(86400)
	ticker1 := time.NewTicker(updateGeoPeriod)
	go func() {
		for range ticker1.C {
			newGeoInfo, err := loadGeoInfo(cgiTimeoutSeconds)
			if err != nil {
				glog.Warning("update geo info failed")
			} else {
				geoMu.Lock()
				geoInfo = newGeoInfo
				geoMu.Unlock()
			}
		}
	}()

	// load idc info
	idcUrlFormat := "http://%s/cgi-bin/tars_oss/cgi-bin/idc_query_int.cgi?user=cloudywu"
	idcInfo, err = dnslink.LoadIdcInfoMapFromCgi(dbHelper, idcUrlFormat, cgiTimeoutSeconds)
	if err != nil {
		glog.Fatal("load idc Info failed")
	}
	updateIdcPeriod := time.Second * cfg.Section("server").Key("idcUpdatePeriod").MustDuration(3600)
	ticker3 := time.NewTicker(updateIdcPeriod)
	go func() {
		for range ticker3.C {
			newIdcInfo, err := dnslink.LoadIdcInfoMapFromCgi(dbHelper, idcUrlFormat, cgiTimeoutSeconds)
			if err != nil {
				glog.Warning("update idc info failed")
			} else {
				idcMu.Lock()
				idcInfo = newIdcInfo
				idcMu.Unlock()
			}
		}
	}()

	validIspIds, err := loadValidIdMap(cfg, "validIspIds")
	if err != nil {
		glog.Fatal("load valid isp ids failed")
	}
	validNationIds, err := loadValidIdMap(cfg, "validNationIds")
	if err != nil {
		glog.Fatal("load valid nation id failed")
	}
	updateLinkData(dbHelper, cgiTimeoutSeconds, validIspIds, validNationIds)
	// update link data periodly
	updateLinkPeriod := time.Second * cfg.Section("server").Key("linkUpdatePeriod").MustDuration(60)
	ticker2 := time.NewTicker(updateLinkPeriod)
	go func() {
		for range ticker2.C {
			updateLinkData(dbHelper, cgiTimeoutSeconds, validIspIds, validNationIds)
		}
	}()
}

func isDetectLinksChangedHandler(c *gin.Context) {
	rVersionId, err := strconv.ParseInt(c.Query("version_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"errno": 1, "error": "no valid version id"})
	} else {
		changeFlag := false
		linkMu.RLock()
		if rVersionId != linkDataVersionId {
			changeFlag = true
			rVersionId = linkDataVersionId
		}
		linkMu.RUnlock()
		c.JSON(http.StatusOK, gin.H{"errno": 0, "error": "", "is_changed": changeFlag, "version_id": rVersionId})
	}
}

func queryDetectLinksHandler(c *gin.Context) {
	linkMu.RLock()
	links := gLinkNameData
	linkMu.RUnlock()
	c.JSON(http.StatusOK, gin.H{"errno": 0, "error": "", "links": links})
}

func postLinkDataHandler(c *gin.Context) {
	var postLinks []linkdb.PostLink
	if err := c.ShouldBindJSON(&postLinks); err != nil {
		glog.Warningf("decode json failed for %s", err.Error())
		c.JSON(http.StatusOK, gin.H{"errno": 1, "error": "decode post json failed"})
	} else {
		var postLinkIds []linkdb.PostLinkId
		geoMu.RLock()
		tmpGeoInfo := geoInfo
		geoMu.RUnlock()
		idcMu.RLock()
		tmpIdcInfo := idcInfo
		idcMu.RUnlock()
		for _, plink := range postLinks {
			linkId, err := plink.TransformToIdInfo(tmpGeoInfo, tmpIdcInfo.IdcName2Id)
			if err == nil {
				var postLinkId linkdb.PostLinkId
				postLinkId.ExceptionMask = plink.ExceptionMask
				postLinkId.DnsCoverLinkIdInfo = linkId
				postLinkIds = append(postLinkIds, postLinkId)
				glog.Infof("post link:  nation:%s, province: %s, isp:%s, idc:%s, mask:%d", plink.NationName, plink.ProvinceName, plink.IspName, plink.IdcName, plink.ExceptionMask)
			} else {
				glog.Warningf("invalid links change info %v", plink)
			}
		}
		if len(postLinkIds) == 0 {
			glog.Warning("has no valid links change")
			c.JSON(http.StatusOK, gin.H{"errno": 2, "error": "no valid links"})
			return
		}
		rowCount, err := linkdb.UpdateLinkMask(dbHelper, postLinkIds)
		if err != nil {
			glog.Errorf("update link mask failed for %s", err.Error())
			c.JSON(http.StatusOK, gin.H{"errno": 3, "error": "internal error"})
		} else {
			glog.Infof("update %d link mask success", rowCount)
			c.JSON(http.StatusOK, gin.H{"errno": 0, "error": ""})
		}
	}
}

func main() {
	gin.DisableConsoleColor()
	router := gin.New()
	logFlushDuration := cfg.Section("glog").Key("logFlushSecond").MustDuration(1 * time.Second)
	logger := common.GinGLogger(logFlushDuration)

	router.Use(logger)
	router.Use(gin.Recovery())
	router.GET("/is_detect_link_changed", isDetectLinksChangedHandler)
	router.GET("/query_detect_links", queryDetectLinksHandler)
	router.POST("/post_detect_links_change", postLinkDataHandler)

	listenIP, err := common.GetIPByInterfaceName(cfg.Section("server").Key("listenInterface").String())
	if err != nil {
		glog.Fatal("get ip address failed")
	}
	listenPort := cfg.Section("server").Key("listenPort").MustInt(12365)
	glog.Info("start server")
	router.Run(fmt.Sprintf("%s:%d", listenIP.String(), listenPort))
}

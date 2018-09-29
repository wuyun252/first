package dnslink

import (
	"common"
	"fmt"
	"github.com/golang/glog"
	"time"
	"strconv"
	. "links_manage/db_operation"
)


type IdcIp struct {
	InnerIp int64  `json:"inner_ip"`
	Enabled int64  `json:"enabled"`
	Ip      string `json:"ip"`
	Vip     string `json:"vip"`
	IpType  int64  `json:"type"`
}

type Idc struct {
	CityId     int64   `json:"city_id"`
	CityName   string  `json:"city_name"`
	IdcId      int64   `json:"idc_id"`
	IdcName    string  `json:"idc_name"`
	IspId      int64   `json:"isp_id"`
	IspName    string  `json:"isp_name"`
	NationId   int64   `json:"nation_id"`
	NationName string  `json:"nation_name"`
	ProId      int64   `json:"pro_id"`
	ProName    string  `json:"pro_name"`
	Ips        []IdcIp `json:"ips"`
}

type DnsCover struct {
	AreaId       int64  `json:"area_id"`
	AreaName     string `json:"area_name"`
	CityId       int64  `json:"city_id"`
	CityName     string  `json:"city_name"`
	IspId        int64  `json:"isp_id"`
	IspName      string `json:"isp_name"`
	NationId     int64  `json:"nation_id"`
	NationName   string `json:"nation_name"`
	Priority     int64  `json:"priority"`
	ResGroupId   int64  `json:"resgrp_id"`
	ResGroupName string `json:"resgrp_name"`
	ResGroupType int64  `json:"resgrp_type"`
	Weight       string `json:"weight"`
	Idcs         []Idc  `json:"idcs"`
}

type DnsCoverInfo struct {
	ResId       int64      `json:"res_id"`
	DetailCover []DnsCover `json:"cover"`
}

type DnsCoverInfoResponse struct {
	Errno int64          `json:"errno"`
	Error string         `json:"error"`
	Seq   int64          `json:"seq"`
	Data  []DnsCoverInfo `json:"data"`
}

func (res *DnsCoverInfoResponse) GetErrno() int64 {
	return res.Errno
}

func (res *DnsCoverInfoResponse) GetError() string {
	return res.Error
}

// get areaid to proids map
func getAreaId2ProIds(url string, timeout time.Duration) (map[int64][]int64, error) {
	geoResp, err := common.GetClusterGeoInfoByCgi(url, timeout)
	if err != nil {
		glog.Errorf("get cluster geo info from %s failed", url)
		return nil,  err
	}
	areaId2ProIds := make(map[int64][]int64)
	for _, proInfo := range geoResp.Data.Province {
		areaId, err := strconv.ParseInt(proInfo.AreaIdString, 10, 64)
		if err == nil {
			areaId2ProIds[areaId] = append(areaId2ProIds[areaId], proInfo.Id)
		}
	}
	return areaId2ProIds, nil
}

// for every domain for res-id
func getDnsCoverLinks(url string, timeout time.Duration, areaId2ProIds map[int64][]int64) (map[DnsCoverLinkIdInfo]bool, error) {
	var respJson DnsCoverInfoResponse
	err := common.GetJsonResponseFromCgi(url, timeout, &respJson)
	if err != nil {
		return nil, err
	}
	result := make(map[DnsCoverLinkIdInfo]bool)
	for _, resData := range respJson.Data {
		for _, dnsCover := range resData.DetailCover {
			// cover with province
			var proIds []int64
			if dnsCover.AreaId < 0 {
				proIds = append(proIds, -dnsCover.AreaId)
			} else {
				var ok bool
				proIds, ok = areaId2ProIds[dnsCover.AreaId]
				if !ok {
					glog.Warningf("area id :%d not in areaid2proid map", dnsCover.AreaId)
					continue
				}
			}
			for _, proName := range proIds {
				for _, idc := range dnsCover.Idcs {
					link := DnsCoverLinkIdInfo{IspId: dnsCover.IspId, NationId: dnsCover.NationId, IdcId: idc.IdcId, ProvinceId: proName}
					result[link] = true
				}
			}
		}
	}
	return result, nil
}

type LinkItem struct {
	links map[DnsCoverLinkIdInfo]bool
	err error
}

func getClusterAllLinks(clusterOssIp string, timeout time.Duration, validIspIds , validNationIds map[int64]bool) (map[DnsCoverLinkIdInfo]bool, error) {
	resUrl := fmt.Sprintf("http://%s/cgi-bin/tars_oss/cgi-bin/get_zone_info_int.cgi?user=cloudywu", clusterOssIp)
	resIds, err := getResIds(resUrl, timeout)
	result := make(map[DnsCoverLinkIdInfo]bool)
	if err != nil {
		glog.Errorf("get res-id from %s failed", resUrl)
		return nil, err
	} else {
		geoUrl := fmt.Sprintf("http://%s/cgi-bin/tars_oss/cgi-bin/get_cdns_basic_info_int.cgi?info_type=province&user=cloudywu", clusterOssIp)
		areaId2ProIds,  err := getAreaId2ProIds(geoUrl, timeout)
		if err != nil {
			glog.Errorf("get geo info from %s failed", geoUrl)
			return nil, err
		}
		linkChan := make(chan LinkItem, len(resIds))
		for resId := range resIds {
			go func(resId int64) {
				coverUrl := fmt.Sprintf("http://%s/cgi-bin/tars_oss/cgi-bin/get_res_cover_info_int.cgi?user=cloudywu&charip=1&res_id=%d", clusterOssIp, resId)
				var it LinkItem
				it.links, it.err = getDnsCoverLinks(coverUrl, timeout, areaId2ProIds)
				linkChan <- it
			}(resId)
		}
		for range resIds {
			linkItem := <-linkChan
			if linkItem.err != nil {
				return nil, fmt.Errorf("get cover failed form some res")
			} else {
				for link := range linkItem.links {
					if validIspIds[link.IspId] && validNationIds[link.NationId] {
						result[link] = true
					}
				}
			}
		}
	}
	return result, nil
}


func GetAllLinks(dbHelper *common.DBHelper, timeout time.Duration, validIspIds , validNationIds map[int64]bool) (map[DnsCoverLinkIdInfo]bool,  error) {
	ossIps, err :=	GetClusterOssIps(dbHelper)
	if err != nil {
		glog.Error("get cluster oss ip info failed")
		return nil, err
	}
	result := make(map[DnsCoverLinkIdInfo]bool)
	dataChan := make(chan LinkItem, len(ossIps))
	for _, ossIpInfo := range ossIps {
		go func(ossIpInfo OssDb) {
			links, err := getClusterAllLinks(ossIpInfo.Master, timeout, validIspIds , validNationIds)
			if err != nil {
				 links, err = getClusterAllLinks(ossIpInfo.Slaver, timeout, validIspIds , validNationIds)
				if err != nil {
					glog.Errorf("get cluster link failed for master: %s, slave:%s", ossIpInfo.Master, ossIpInfo.Slaver)
					dataChan <- LinkItem{links:nil, err:err}
				} else {
					dataChan <- LinkItem{links: links, err: nil}
				}
			} else {
				dataChan <- LinkItem{links:links, err:nil}
			}
		}(ossIpInfo)
	}
	for range ossIps {
		linkData := <-dataChan
		if linkData.err != nil {
			return nil, fmt.Errorf("get links failed for some cluster")
		}
		for link := range linkData.links {
			result[link] = true
		}
	}
	return result, nil
}

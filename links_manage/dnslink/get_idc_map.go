package dnslink

import (
	"time"
	"github.com/golang/glog"
	"common"
	. "links_manage/db_operation"
	"fmt"
)

type IdcIdNameMap struct {
	IdcId2Name map[int64]string
	IdcName2Id map[string]int64
}

type IdcIdName struct {
	Id   int64  `json:"id"`
	Name string `json:"idcName"`
}

type IdcIdNameResponse struct {
	common.CgiResponseCommonMember
	Data []IdcIdName `json:"data"`
}

func GetClusterIdcInfoFromCgi(url string, timeout time.Duration) (*IdcIdNameResponse, error) {
	var idcResp IdcIdNameResponse
	err := common.GetJsonResponseFromCgi(url, timeout, &idcResp)
	if err != nil {
		glog.Errorf("get cluster idc info from %s failed", url)
		return nil, err
	}
	return &idcResp, nil
}

type IdcRespWithError struct {
	idcResp *IdcIdNameResponse
	err error
}

// merge all idc info of cluster
func LoadIdcInfoMapFromCgi(dbHelper *common.DBHelper, urlFormat string, timeout time.Duration) (*IdcIdNameMap, error) {
	ossDbs, err := GetClusterOssIps(dbHelper)
	if err != nil {
		glog.Fatal("get cluster oss ip failed")
		return nil, fmt.Errorf("get cluster oss ip failed")
	}
	var idcInfo IdcIdNameMap
	dataChan := make(chan IdcRespWithError, len(ossDbs))
	for _, ossIps := range ossDbs {
		go func(ossIpPair OssDb) {
			clusterOssIps := []string{ossIpPair.Master, ossIpPair.Slaver}
			succFlag := false
			for _, cip := range clusterOssIps {
				url := fmt.Sprintf(urlFormat, cip)
				idcResp, err := GetClusterIdcInfoFromCgi(url, timeout)
				if err == nil {
					dataChan <- IdcRespWithError{idcResp:idcResp, err:nil}
					succFlag = true
					break
				} else {
					glog.Errorf("get cluster idc info from oss %s failed", cip)
				}
			}
			if !succFlag {
				glog.Error("get cluster idc failed from both oss ip")
				dataChan <- IdcRespWithError{idcResp:nil, err:fmt.Errorf("get cluster idc failed")}
			}
		}(ossIps)
	}
	idcInfo.IdcName2Id = make(map[string]int64)
	idcInfo.IdcId2Name = make(map[int64]string)
	for range ossDbs {
		idcItem := <-dataChan
		if idcItem.err != nil {
			return nil, err
		} else {
			for _, idc := range idcItem.idcResp.Data {
				idcInfo.IdcId2Name[idc.Id] = idc.Name
				idcInfo.IdcName2Id[idc.Name] = idc.Id
			}
		}
	}
	return &idcInfo, nil
}
